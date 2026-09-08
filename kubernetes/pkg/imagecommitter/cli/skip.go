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
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/alibaba/OpenSandbox/sandbox-k8s/pkg/imagecommitter"
)

// skipContainersEnv overrides the default skip list with a comma-separated list
// of container names. An empty value commits every container.
const skipContainersEnv = "SNAPSHOT_SKIP_CONTAINERS"

// restoredContainerName is the container a snapshot is restored from. It is the
// same name the lifecycle server selects a restore image by, so it can never be
// skipped: the job would report success for a snapshot that cannot be restored,
// and nothing downstream re-checks that a restore image was actually pushed.
const restoredContainerName = "sandbox"

// defaultSkippedContainers are the containers a sandbox snapshot never restores
// from. The egress sidecar and the execd bootstrap init container are recreated
// from their own pinned images, so committing and pushing a per-sandbox copy of
// them is work whose output nothing ever reads.
var defaultSkippedContainers = []string{"egress", "execd-installer"}

// applySkipList drops the containers a snapshot is never restored from, before
// any container is paused, and reports what it dropped.
//
// Both requests are filtered by the same list. The unpause path matters as much
// as the commit path: a skipped container was never paused, and unpausing a
// running container fails.
func applySkipList(
	operation string,
	commit imagecommitter.CommitRequest,
	unpause imagecommitter.UnpauseRequest,
	output io.Writer,
) (imagecommitter.CommitRequest, imagecommitter.UnpauseRequest, error) {
	skipped, err := skippedContainerNames()
	if err != nil {
		return commit, unpause, err
	}

	if operation == "unpause" {
		kept, dropped := partitionContainerNames(unpause.ContainerNames, skipped)
		for _, name := range dropped {
			fmt.Fprintf(output, "Skipping unpause for container %q: it is never paused for a snapshot\n", name)
		}
		unpause.ContainerNames = kept
		return commit, unpause, nil
	}

	kept, dropped := partitionContainerSpecs(commit.Containers, skipped)
	for _, spec := range dropped {
		fmt.Fprintf(output, "Skipping container %q: not restored from a snapshot image\n", spec.Name)
	}
	if len(kept) == 0 {
		return commit, unpause, fmt.Errorf(
			"every requested container is on the %s skip list; nothing to commit",
			skipContainersEnv,
		)
	}
	commit.Containers = kept
	return commit, unpause, nil
}

// skippedContainerNames returns the containers this committer must not commit,
// push, pause, or unpause, and fails closed on a skip list that would drop the
// restored container. Refusing is deliberate: a rejected configuration is
// visible, whereas one that is silently corrected is one somebody still
// believes is in effect.
func skippedContainerNames() (map[string]bool, error) {
	names := defaultSkippedContainers
	if raw, ok := os.LookupEnv(skipContainersEnv); ok {
		names = strings.Split(raw, ",")
	}

	skipped := make(map[string]bool, len(names))
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		if trimmed == restoredContainerName {
			return nil, fmt.Errorf(
				"%s must not contain %q: a snapshot is restored from that container, so skipping it would report a snapshot that cannot be restored",
				skipContainersEnv,
				restoredContainerName,
			)
		}
		skipped[trimmed] = true
	}
	return skipped, nil
}

// partitionContainerSpecs splits specs into the ones to snapshot and the ones to
// skip, preserving the caller's order in both.
func partitionContainerSpecs(
	specs []imagecommitter.ContainerSpec,
	skipped map[string]bool,
) ([]imagecommitter.ContainerSpec, []imagecommitter.ContainerSpec) {
	var keep, drop []imagecommitter.ContainerSpec
	for _, spec := range specs {
		if skipped[spec.Name] {
			drop = append(drop, spec)
			continue
		}
		keep = append(keep, spec)
	}
	return keep, drop
}

// partitionContainerNames is partitionContainerSpecs for the bare container
// names the unpause operation takes.
func partitionContainerNames(names []string, skipped map[string]bool) ([]string, []string) {
	var keep, drop []string
	for _, name := range names {
		if skipped[name] {
			drop = append(drop, name)
			continue
		}
		keep = append(keep, name)
	}
	return keep, drop
}
