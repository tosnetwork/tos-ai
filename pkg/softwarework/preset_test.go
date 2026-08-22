package softwarework

import "testing"

func TestFrozenV1PresetReturnsIndependentAuthorityCollections(t *testing.T) {
	firstPolicy, secondPolicy := FrozenV1Policy(), FrozenV1Policy()
	delete(firstPolicy.AllowedImages, FrozenV1ImageDigest)
	if _, ok := secondPolicy.AllowedImages[FrozenV1ImageDigest]; !ok {
		t.Fatal("one runtime mutated another runtime's frozen image authority")
	}
	firstContract, secondContract := FrozenV1Contract(), FrozenV1Contract()
	firstContract.Arguments[0] = "substituted"
	if secondContract.Arguments[0] != "test" || secondContract.ManifestDigest != FrozenV1ManifestDigest ||
		secondContract.ToolchainDigest != FrozenV1ImageDigest || secondContract.Limits != FrozenV1Limits() {
		t.Fatal("frozen V1 contract is mutable across callers")
	}
}
