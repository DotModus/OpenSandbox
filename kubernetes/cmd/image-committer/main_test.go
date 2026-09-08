// Copyright 2025 Alibaba Group Holding Ltd.
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

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetImageDigestReturnsErrorOnInspectFailure(t *testing.T) {
	original := commandCombinedOutput
	t.Cleanup(func() { commandCombinedOutput = original })
	commandCombinedOutput = func(_ string, _ ...string) ([]byte, error) {
		return []byte("inspect failed"), errors.New("exit status 1")
	}

	digest, err := getImageDigest("registry.example.com/test/image:snap")

	if err == nil {
		t.Fatal("expected digest extraction error")
	}
	if digest != "" {
		t.Fatalf("expected empty digest on error, got %q", digest)
	}
	if digest == "sha256:placeholder" {
		t.Fatal("digest extraction must not return placeholder")
	}
}

func TestGetImageDigestReturnsErrorOnEmptyInspectOutput(t *testing.T) {
	original := commandCombinedOutput
	t.Cleanup(func() { commandCombinedOutput = original })
	commandCombinedOutput = func(_ string, _ ...string) ([]byte, error) {
		return []byte(" \n"), nil
	}

	digest, err := getImageDigest("registry.example.com/test/image:snap")

	if err == nil {
		t.Fatal("expected empty digest error")
	}
	if digest != "" {
		t.Fatalf("expected empty digest on error, got %q", digest)
	}
}

func TestGetImageDigestReturnsDigest(t *testing.T) {
	original := commandCombinedOutput
	t.Cleanup(func() { commandCombinedOutput = original })
	commandCombinedOutput = func(_ string, _ ...string) ([]byte, error) {
		return []byte("sha256:abc123\n"), nil
	}

	digest, err := getImageDigest("registry.example.com/test/image:snap")

	if err != nil {
		t.Fatalf("expected digest extraction to succeed, got %v", err)
	}
	if digest != "sha256:abc123" {
		t.Fatalf("unexpected digest %q", digest)
	}
}

func TestGetContainerIDByNerdctlReturnsRunningContainer(t *testing.T) {
	original := commandCombinedOutput
	t.Cleanup(func() { commandCombinedOutput = original })

	calls := 0
	commandCombinedOutput = func(name string, args ...string) ([]byte, error) {
		calls++
		if name != "nerdctl" {
			t.Fatalf("unexpected command %q", name)
		}
		if calls != 1 {
			t.Fatalf("expected a single nerdctl lookup, got %d", calls)
		}
		return []byte("container-running\n"), nil
	}

	containerID, err := getContainerIDByNerdctl("pod-1", "default", "sandbox")
	if err != nil {
		t.Fatalf("expected running container lookup to succeed, got %v", err)
	}
	if containerID != "container-running" {
		t.Fatalf("unexpected container ID %q", containerID)
	}
}

func TestGetContainerIDByNerdctlFallsBackToStoppedContainers(t *testing.T) {
	original := commandCombinedOutput
	t.Cleanup(func() { commandCombinedOutput = original })

	var calls [][]string
	commandCombinedOutput = func(name string, args ...string) ([]byte, error) {
		if name != "nerdctl" {
			t.Fatalf("unexpected command %q", name)
		}
		calls = append(calls, append([]string(nil), args...))
		switch len(calls) {
		case 1:
			return []byte("\n"), nil
		case 2:
			return []byte("container-stopped\n"), nil
		default:
			t.Fatalf("unexpected extra nerdctl lookup #%d", len(calls))
			return nil, nil
		}
	}

	containerID, err := getContainerIDByNerdctl("pod-1", "default", "sandbox")
	if err != nil {
		t.Fatalf("expected stopped container fallback to succeed, got %v", err)
	}
	if containerID != "container-stopped" {
		t.Fatalf("unexpected container ID %q", containerID)
	}
	if len(calls) != 2 {
		t.Fatalf("expected two nerdctl lookups, got %d", len(calls))
	}
	if contains(calls[0], "-a") {
		t.Fatalf("first lookup should only inspect running containers: %v", calls[0])
	}
	if !contains(calls[1], "-a") {
		t.Fatalf("second lookup should include stopped containers: %v", calls[1])
	}
}

func TestGetContainerIDByNerdctlReturnsHelpfulErrorWhenBothLookupsAreEmpty(t *testing.T) {
	original := commandCombinedOutput
	t.Cleanup(func() { commandCombinedOutput = original })

	commandCombinedOutput = func(_ string, _ ...string) ([]byte, error) {
		return []byte("\n"), nil
	}

	_, err := getContainerIDByNerdctl("pod-1", "default", "sandbox")
	if err == nil {
		t.Fatal("expected lookup failure when both running and stopped container searches are empty")
	}
	if got := err.Error(); got != "container 'sandbox' not found in pod default/pod-1 (nerdctl ps and nerdctl ps -a returned empty)" {
		t.Fatalf("unexpected error %q", got)
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestWriteSnapshotResultWritesTerminationMessage(t *testing.T) {
	original := terminationMessagePath
	t.Cleanup(func() { terminationMessagePath = original })
	terminationMessagePath = filepath.Join(t.TempDir(), "termination.log")

	err := writeSnapshotResult(
		[]ContainerSpec{
			{Name: "main", URI: "registry.example.com/main:snap"},
			{Name: "sidecar", URI: "registry.example.com/sidecar:snap"},
		},
		map[string]string{
			"main":    "sha256:main",
			"sidecar": "sha256:sidecar",
		},
		nil,
	)
	if err != nil {
		t.Fatalf("writeSnapshotResult failed: %v", err)
	}

	data, err := os.ReadFile(terminationMessagePath)
	if err != nil {
		t.Fatalf("failed to read termination message: %v", err)
	}

	var result snapshotResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("termination message is not valid JSON: %v", err)
	}
	if len(result.Containers) != 2 {
		t.Fatalf("expected 2 container results, got %d", len(result.Containers))
	}
	if result.Containers[0].Name != "main" || result.Containers[0].Digest != "sha256:main" {
		t.Fatalf("unexpected first result: %#v", result.Containers[0])
	}
	if result.Containers[1].Name != "sidecar" || result.Containers[1].Digest != "sha256:sidecar" {
		t.Fatalf("unexpected second result: %#v", result.Containers[1])
	}
	if len(result.Skipped) != 0 {
		t.Fatalf("expected no skipped containers, got %v", result.Skipped)
	}
}

func TestWriteSnapshotResultRecordsSkippedContainers(t *testing.T) {
	original := terminationMessagePath
	t.Cleanup(func() { terminationMessagePath = original })
	terminationMessagePath = filepath.Join(t.TempDir(), "termination.log")

	err := writeSnapshotResult(
		[]ContainerSpec{{Name: "sandbox", URI: "registry.example.com/sandbox:snap"}},
		map[string]string{"sandbox": "sha256:sandbox"},
		[]ContainerSpec{{Name: "egress", URI: "registry.example.com/egress:snap"}},
	)
	if err != nil {
		t.Fatalf("writeSnapshotResult failed: %v", err)
	}

	data, err := os.ReadFile(terminationMessagePath)
	if err != nil {
		t.Fatalf("failed to read termination message: %v", err)
	}

	var result snapshotResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("termination message is not valid JSON: %v", err)
	}
	if len(result.Containers) != 1 || result.Containers[0].Name != "sandbox" {
		t.Fatalf("expected only the sandbox container result, got %#v", result.Containers)
	}
	if len(result.Skipped) != 1 || result.Skipped[0] != "egress" {
		t.Fatalf("expected egress recorded as skipped, got %v", result.Skipped)
	}
}

func TestSkippedContainerNamesDefaultsToSidecarAndInit(t *testing.T) {
	// t.Setenv registers the restore; Unsetenv then gives this test a
	// genuinely unset variable without leaking it into the rest of the package.
	t.Setenv("SNAPSHOT_SKIP_CONTAINERS", "")
	os.Unsetenv("SNAPSHOT_SKIP_CONTAINERS")

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
	if skipped["sandbox"] {
		t.Fatal("the sandbox container must never be skipped by default")
	}
}

func TestSkippedContainerNamesHonoursOverride(t *testing.T) {
	t.Setenv("SNAPSHOT_SKIP_CONTAINERS", " proxy , , logger ")

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
	t.Setenv("SNAPSHOT_SKIP_CONTAINERS", "")

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
	for _, value := range []string{
		"sandbox",
		"egress,sandbox",
		" sandbox , egress ",
	} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("SNAPSHOT_SKIP_CONTAINERS", value)

			skipped, err := skippedContainerNames()

			if err == nil {
				t.Fatal("skipping the restored container must be refused")
			}
			if skipped != nil {
				t.Fatalf("expected no skip set on refusal, got %v", skipped)
			}
			if !strings.Contains(err.Error(), "sandbox") {
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

func TestPartitionContainerSpecsKeepsSandboxAndSkipsSidecars(t *testing.T) {
	specs := []ContainerSpec{
		{Name: "sandbox", URI: "registry.example.com/sandbox:snap"},
		{Name: "egress", URI: "registry.example.com/egress:snap"},
		{Name: "execd-installer", URI: "registry.example.com/execd:snap"},
	}

	skipped, err := skippedContainerNames()
	if err != nil {
		t.Fatalf("default skip list must be accepted, got %v", err)
	}

	commit, skip := partitionContainerSpecs(specs, skipped)

	if len(commit) != 1 || commit[0].Name != "sandbox" {
		t.Fatalf("expected only the sandbox container to be committed, got %#v", commit)
	}
	if len(skip) != 2 || skip[0].Name != "egress" || skip[1].Name != "execd-installer" {
		t.Fatalf("expected both sidecars skipped in order, got %#v", skip)
	}
}

func TestPartitionContainerNamesSkipsSidecarsForUnpause(t *testing.T) {
	skipped, err := skippedContainerNames()
	if err != nil {
		t.Fatalf("default skip list must be accepted, got %v", err)
	}

	keep, skip := partitionContainerNames([]string{"sandbox", "egress"}, skipped)

	if len(keep) != 1 || keep[0] != "sandbox" {
		t.Fatalf("expected only the sandbox container to be unpaused, got %v", keep)
	}
	if len(skip) != 1 || skip[0] != "egress" {
		t.Fatalf("expected the egress sidecar to be skipped, got %v", skip)
	}
}
