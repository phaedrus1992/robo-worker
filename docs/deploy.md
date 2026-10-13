# Deploy

The service runs in the `phaedrus` namespace of the k0s cluster on the agent host.
Nothing is installed on the host: no unix account, no systemd unit, and no checkout that the service reads.
The image is the unit of release.

The service has two parts, from one image:

- The control plane: one `Deployment` with one replica. It serves MCP, keeps the queues, runs `llmenv export`, and creates the job pods.
- The job pods: one k8s `Job` for each attempt of a task, named `robo-worker-<job-id>-<attempt>`. Each one runs `claude -p` and writes its outcome to the state claim.

## One-time cluster setup (ranger)

The namespace exists already and carries the access this work uses.
Do these steps one time.

1. Create the three secrets. None of them is in git.

       kubectl -n phaedrus create secret generic robo-worker-secrets \
         --from-file=callers.toml=<callers file> --from-file=claude-oauth-token=<token file>
       kubectl -n phaedrus create secret generic robo-worker-gh \
         --from-file=hosts.yml=<the gh hosts.yml for RoboPhaedrus1992>
       kubectl -n phaedrus create secret generic robo-worker-git \
         --from-file=gitconfig=<the worker gitconfig> --from-file=signing-key=<the worker signing key>

   - `robo-worker-secrets`: `callers.toml` holds a `[callers]` table with one `name = "token"` line per caller. Make each token with `uv run python3 -c 'import secrets; print(secrets.token_urlsafe(32))'`. `claude-oauth-token` is the output of `claude setup-token`. Only the control plane mounts this secret.
   - `robo-worker-gh`: `hosts.yml` from `gh auth login` as `RoboPhaedrus1992`.
   - `robo-worker-git`: the worker's `gitconfig` and its ssh signing key. The `gitconfig` sets `user.name`, `user.email`, `gpg.format = ssh`, `user.signingkey = /etc/robo-worker/git/signing-key`, and `commit.gpgsign = true`.

   The pods mount these files with mode `0440` and read them as group 10001 through `fsGroup`.
   The service accepts a file that its own group can read, and refuses a file that others can read.

2. Check that the cluster can pull `ghcr.io/phaedrus1992/robo-worker`.
   CI pushes the image from `main`.
   If the package is private, make it public in the package settings on GitHub, or add an image pull secret to `deploy/k8s/deployment.yaml` and `deploy/k8s/job-pod.json`.

3. Apply the manifests and wait for the rollout:

       scripts/deploy.sh

4. Put the llmenv config on the state claim, one time.
   llmenv keeps its config in a git repo and runs `git pull` on it at each export, so the config cannot be a read-only `Secret`.
   `RoboPhaedrus1992` needs read access to the config repo.
   Use a config with its own scope for the worker, and do not enable the bundle that sets the `ben` output style.

       kubectl -n phaedrus exec deploy/robo-worker -- \
         git clone https://github.com/<owner>/<llmenv config repo>.git /state/home/.config/llmenv

   Then check one export where the service runs it:
   `kubectl -n phaedrus exec deploy/robo-worker -- llmenv export`.

5. Check the health:
   `kubectl -n phaedrus get --raw /api/v1/namespaces/phaedrus/services/robo-worker:8765/proxy/health`

## Callers

- ranger's Mac: `claude mcp add --transport http robo-worker http://192.168.211.10:30875/mcp --header "Authorization: Bearer <mac token>"`.
- The agent: part B of epic #2 adds the MCP client config.

## Deploy

    scripts/deploy.sh [--force] [git-ref]

The script fetches, checks the ref out into a deploy worktree, applies `deploy/k8s` from it, waits for the rollout, and checks `/health`.
When either fails, it undoes the rollout to the revision that ran before.
It refuses to deploy while a job `Job` is active, or when it cannot count them, unless you pass `--force`.
A rollout does not stop a job pod: the new control plane attaches to each running job.

Exit codes: 0 deployed, 2 usage, 3 rolled back, 4 rollback failed or nothing to roll back to, 5 refused, 6 nothing changed (cannot fetch, check out, or read the Deployment).

The manifests in the ref name the image tag.
A release commit sets `images.newTag` in `deploy/k8s/kustomization.yaml` to the version.

## Operations

- Health: `GET /health` needs no token. It returns 503 when a job cannot run. The pod probes are `tcpSocket` checks, so a 503 does not restart the pod.
- Storage: the `StorageClass` has `reclaimPolicy: Delete`. Do not delete a `robo-worker-*` claim: that deletes the job store, the worker home, or the checkouts.
- Job files: `<state_root>/jobs/<id>/` holds `job.json`, `events.jsonl`, `run_spec.json`, `stream.jsonl`, `stderr.log`, `mcp.json`, `progress_token`, and `outcome.json`. Read them with `kubectl -n phaedrus exec deploy/robo-worker -- cat /state/jobs/<id>/events.jsonl`.
- Pod logs: `kubectl -n phaedrus logs job/robo-worker-<job-id>-<attempt>`. A finished `Job` stays for one hour. A job's attempts: `kubectl -n phaedrus get jobs -l robo-worker/job-id=<job-id>`.
- Usage: `<state_root>/usage.jsonl` has one line for each job run and each ask.
- A paused repo queue (`checkout_dirty`): clean the checkout under `workspace_root`, then call `resume_queue`.
- An auth pause: replace the `claude-oauth-token` key of `robo-worker-secrets`, update `auth.oauth_token_created` in `deploy/k8s/config.toml`, and run `scripts/deploy.sh`.
- A job pod that is gone without an outcome: the control plane sets `interrupted` and resumes the session in a new pod, until the job reaches `max_interruptions`.
- A job pod that runs past `activeDeadlineSeconds` (`max_job_seconds` plus a start margin) fails with reason `timeout`, and no new pod starts.
