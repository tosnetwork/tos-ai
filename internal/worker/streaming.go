package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"connectrpc.com/connect"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
)

const (
	defaultStreamChunkBytes = 64 << 10
	maximumStreamChunkBytes = 1 << 20
)

// InvokeStream executes once under the durable unary task state machine, then
// streams that retained terminal result with transport backpressure. Client
// disconnect does not cancel/re-execute the durable task.
func (s *Service) InvokeStream(
	_ context.Context,
	request *connect.Request[edgev1.InvokeStreamRequest],
	stream *connect.ServerStream[edgev1.StreamEvent],
) error {
	if request == nil || request.Msg == nil || request.Msg.Invocation == nil || stream == nil {
		return invalidArgument(errors.New("empty streaming invocation"))
	}
	invocation := request.Msg.Invocation
	chunkBytes, err := streamChunkLimit(request.Msg.MaxChunkBytes, invocation.MaxOutputBytes)
	if err != nil {
		return invalidArgument(err)
	}
	deadline := time.UnixMilli(invocation.DeadlineUnixMillis)
	executionContext, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	response, invokeErr := s.Invoke(executionContext, connect.NewRequest(invocation))
	if invokeErr == nil {
		return sendStreamResult(stream, invocation, response.Msg, 0, 0, chunkBytes, "")
	}
	task, taskErr := s.GetTask(context.Background(), connect.NewRequest(&edgev1.GetTaskRequest{
		RequestId: invocation.RequestId, TaskId: invocation.TaskId,
		RequestDigest:         invocation.RequestDigest,
		RetainUntilUnixMillis: invocation.RetainUntilUnixMillis,
	}))
	if taskErr != nil {
		return invokeErr
	}
	return sendRetainedTask(stream, invocation, task.Msg, 0, 0, chunkBytes, "")
}

func (s *Service) ResumeStream(
	_ context.Context,
	request *connect.Request[edgev1.ResumeStreamRequest],
	stream *connect.ServerStream[edgev1.StreamEvent],
) error {
	if request == nil || request.Msg == nil || request.Msg.Task == nil || stream == nil {
		return invalidArgument(errors.New("empty stream resume"))
	}
	chunkBytes, err := streamChunkLimit(request.Msg.MaxChunkBytes, ^uint64(0))
	if err != nil || request.Msg.NextSequence == 0 || request.Msg.NextOffset == 0 ||
		!validRequestDigest(request.Msg.ExpectedStreamDigest) {
		return invalidArgument(errors.New("invalid stream resume cursor"))
	}
	task, err := s.GetTask(context.Background(), connect.NewRequest(request.Msg.Task))
	if err != nil {
		return err
	}
	invocation := &edgev1.InvokeRequest{
		RequestId: request.Msg.Task.RequestId, TaskId: request.Msg.Task.TaskId,
		RequestDigest:         request.Msg.Task.RequestDigest,
		RetainUntilUnixMillis: request.Msg.Task.RetainUntilUnixMillis,
	}
	return sendRetainedTask(
		stream, invocation, task.Msg, request.Msg.NextSequence,
		request.Msg.NextOffset, chunkBytes, request.Msg.ExpectedStreamDigest,
	)
}

func sendRetainedTask(
	stream *connect.ServerStream[edgev1.StreamEvent], invocation *edgev1.InvokeRequest,
	task *edgev1.GetTaskResponse, sequence, offset, chunkBytes uint64, expectedDigest string,
) error {
	if task == nil || task.RequestId != invocation.RequestId || task.TaskId != invocation.TaskId ||
		task.RequestDigest != invocation.RequestDigest {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("retained task binding mismatch"))
	}
	if task.Status == edgev1.TaskStatus_TASK_STATUS_ACCEPTED || task.Status == edgev1.TaskStatus_TASK_STATUS_RUNNING {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("retained task is not terminal"))
	}
	if task.Status == edgev1.TaskStatus_TASK_STATUS_SUCCEEDED && task.Result != nil {
		return sendStreamResult(stream, invocation, task.Result, sequence, offset, chunkBytes, expectedDigest)
	}
	digest := sha256.Sum256(nil)
	event := &edgev1.StreamEvent{
		RequestId: invocation.RequestId, TaskId: invocation.TaskId,
		RequestDigest: invocation.RequestDigest, Sequence: sequence, Offset: offset,
		StreamDigest: "sha256:" + hex.EncodeToString(digest[:]), Terminal: true,
		TerminalStatus: task.Status, ErrorCode: task.ErrorCode,
		CompletedUnixMillis: task.CompletedUnixMillis,
	}
	return stream.Send(event)
}

func sendStreamResult(
	stream *connect.ServerStream[edgev1.StreamEvent], invocation *edgev1.InvokeRequest,
	result *edgev1.InvokeResponse, sequence, offset, chunkBytes uint64, expectedDigest string,
) error {
	if result == nil || result.Usage == nil || offset > uint64(len(result.Output)) {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("invalid retained stream result"))
	}
	digest := sha256.Sum256(result.Output)
	streamDigest := "sha256:" + hex.EncodeToString(digest[:])
	if expectedDigest != "" && expectedDigest != streamDigest {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("stream digest conflict"))
	}
	for offset < uint64(len(result.Output)) {
		end := offset + chunkBytes
		if end > uint64(len(result.Output)) {
			end = uint64(len(result.Output))
		}
		if err := stream.Send(&edgev1.StreamEvent{
			RequestId: invocation.RequestId, TaskId: invocation.TaskId,
			RequestDigest: invocation.RequestDigest, Sequence: sequence, Offset: offset,
			Chunk:            append([]byte(nil), result.Output[offset:end]...),
			TotalOutputBytes: uint64(len(result.Output)), StreamDigest: streamDigest,
			ModelRevision: result.ModelRevision, RuntimeRevision: result.RuntimeRevision,
		}); err != nil {
			return err
		}
		offset = end
		sequence++
	}
	return stream.Send(&edgev1.StreamEvent{
		RequestId: invocation.RequestId, TaskId: invocation.TaskId,
		RequestDigest: invocation.RequestDigest, Sequence: sequence, Offset: offset,
		TotalOutputBytes: uint64(len(result.Output)), StreamDigest: streamDigest,
		Terminal: true, TerminalStatus: edgev1.TaskStatus_TASK_STATUS_SUCCEEDED,
		Usage: result.Usage, ModelRevision: result.ModelRevision,
		RuntimeRevision:     result.RuntimeRevision,
		CompletedUnixMillis: result.CompletedUnixMillis,
	})
}

func streamChunkLimit(value, maximumOutput uint64) (uint64, error) {
	if value == 0 {
		value = defaultStreamChunkBytes
		if maximumOutput < value {
			value = maximumOutput
		}
	}
	if value == 0 || value > maximumStreamChunkBytes || value > maximumOutput {
		return 0, errors.New("stream chunk limit is outside policy")
	}
	return value, nil
}
