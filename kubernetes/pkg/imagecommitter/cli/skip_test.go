// Copyright 2026 Alibaba Group Holding Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/alibaba/OpenSandbox/sandbox-k8s/pkg/imagecommitter"
)

func TestSkippedContainerNamesDefaultsToSidecarAndInit(t *testing.T) {
	// t.Setenv registers the restore; Unsetenv then gives this test a genuinely
	// unset variable without leaking it into the rest of the package.
	t.Setenv(skipContainersEnv, "")
	os.Unsetenv(skipContainersEnv)

	skipped, err := skippedContainerNames()
	if err != nil {
		t.Fatalf("default skip list must be accepted, got %v", err)
	}
	if !skipped["egress"] {
		t.Fatal("egress sidecar must be skipped by default")
	}
	if !skipped["execd-installer"] {
		t.Fatal("execd bootstrap init container must be skipped by default")
	}
	if skipped[restoredContainerName] {
		t.Fatalf("%q must never be skipped by default", restoredContainerName)
	}
}

func TestSkippedContainerNamesHonoursOverride(t *testing.T) {
	t.Setenv(skipContainersEnv, " proxy , , logger ")

	skipped, err := skippedContainerNames()
	if err != nil {
		t.Fatalf("override must be accepted, got %v", err)
	}
	if len(skipped) != 2 || !skipped["proxy"] || !skipped["logger"] {
		t.Fatalf("unexpected skip set %v", skipped)
	}
	if skipped["egress"] {
		t.Fatal("override must replace the default skip list, not extend it")
	}
}

func TestSkippedContainerNamesEmptyOverrideCommitsEverything(t *testing.T) {
	t.Setenv(skipContainersEnv, "")

	skipped, err := skippedContainerNames()
	if err != nil {
		t.Fatalf("empty override must be accepted, got %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("empty override must commit every container, got %v", skipped)
	}
}

func TestSkippedContainerNamesRefusesToSkipTheRestoredContainer(t *testing.T) {
	// A snapshot is restored from this container and nothing downstream
	// re-checks that its image was pushed, so skipping it would produce a
	// snapshot that reports success and cannot be restored.
	for _, value := range []string{"sandbox", "egress,sandbox", " sandbox , egress "} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(skipContainersEnv, value)

			skipped, err := skippedContainerNames()

			if err == nil {
				t.Fatal("skipping the restored container must be refused")
			}
			if skipped != nil {
				t.Fatalf("expected no skip set on refusal, got %v", skipped)
			}
			if !strings.Contains(err.Error(), restoredContainerName) {
				t.Fatalf("error must name the container, got %q", err)
			}
		})
	}
}

func TestDefaultSkipListNeverContainsTheRestoredContainer(t *testing.T) {
	for _, name := range defaultSkippedContainers {
		if name == restoredContainerName {
			t.Fatalf("default skip list must never contain %q", restoredContainerName)
		}
	}
}

func TestApplySkipListKeepsOnlyTheRestorableContainer(t *testing.T) {
	t.Setenv(skipContainersEnv, "")
	os.Unsetenv(skipContainersEnv)

	commit := imagecommitter.CommitRequest{
		PodName:   "pod-1",
		Namespace: "default",
		Containers: []imagecommitter.ContainerSpec{
			{Name: "sandbox", Target: "registry.example.com/sandbox:snap"},
			{Name: "egress", Target: "registry.example.com/egress:snap"},
			{Name: "execd-installer", Target: "registry.example.com/execd:snap"},
		},
	}
	output := &bytes.Buffer{}

	commit, _, err := applySkipList("commit", commit, imagecommitter.UnpauseRequest{}, output)
	if err != nil {
		t.Fatalf("applySkipList failed: %v", err)
	}

	if len(commit.Containers) != 1 || commit.Containers[0].Name != "sandbox" {
		t.Fatalf("expected only the sandbox container, got %#v", commit.Containers)
	}
	for _, name := range []string{"egress", "execd-installer"} {
		if !strings.Contains(output.String(), name) {
			t.Fatalf("skipped container %q was not reported: %q", name, output.String())
		}
	}
}

func TestApplySkipListFiltersUnpauseNames(t *testing.T) {
	t.Setenv(skipContainersEnv, "")
	os.Unsetenv(skipContainersEnv)

	unpause := imagecommitter.UnpauseRequest{
		PodName:        "pod-1",
		Namespace:      "default",
		ContainerNames: []string{"sandbox", "egress"},
	}
	output := &bytes.Buffer{}

	_, unpause, err := applySkipList("unpause", imagecommitter.CommitRequest{}, unpause, output)
	if err != nil {
		t.Fatalf("applySkipList failed: %v", err)
	}

	if len(unpause.ContainerNames) != 1 || unpause.ContainerNames[0] != "sandbox" {
		t.Fatalf("expected only the sandbox container to be unpaused, got %v", unpause.ContainerNames)
	}
	if !strings.Contains(output.String(), "egress") {
		t.Fatalf("skipped unpause was not reported: %q", output.String())
	}
}

func TestApplySkipListRefusesWhenNothingWouldBeCommitted(t *testing.T) {
	t.Setenv(skipContainersEnv, "egress")

	commit := imagecommitter.CommitRequest{
		Containers: []imagecommitter.ContainerSpec{
			{Name: "egress", Target: "registry.example.com/egress:snap"},
		},
	}

	_, _, err := applySkipList("commit", commit, imagecommitter.UnpauseRequest{}, &bytes.Buffer{})

	if err == nil {
		t.Fatal("committing nothing must be refused rather than reported as success")
	}
}

func TestApplySkipListPropagatesAnInvalidSkipList(t *testing.T) {
	t.Setenv(skipContainersEnv, restoredContainerName)

	_, _, err := applySkipList("commit", imagecommitter.CommitRequest{}, imagecommitter.UnpauseRequest{}, &bytes.Buffer{})

	if err == nil {
		t.Fatal("an invalid skip list must fail the run, not be ignored")
	}
}
