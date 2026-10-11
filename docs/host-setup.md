# Host setup

ranger does these steps one time on the agent host.
The deploy script (`scripts/deploy.sh`) does every later update.

## User and tools

1. Create the user: `sudo useradd --create-home --shell /bin/bash robo-worker`.
2. As `robo-worker`, install uv, the Claude Code CLI (native installer), llmenv, and gh.
   All of them go under `~/.local/bin` or `~/.cargo/bin`, which the unit puts on `PATH`.
3. As `robo-worker`, run `gh auth login` as `RoboPhaedrus1992`, then `gh auth setup-git`.
4. Give `robo-worker` its own commit signing key, and set `user.name`, `user.email`, `gpg.format`, `user.signingkey`, and `commit.gpgsign` in its `~/.gitconfig`.
5. Give `robo-worker` an llmenv config with a `user:robo-worker` scope.
   Do not enable the bundle that sets the `ben` output style.
   Run `llmenv export` in a repo checkout as `robo-worker` and read `LLMENV_ACTIVE_BUNDLES`.

## Secrets

1. Run `claude setup-token` (as any user that can open a browser), and write the token to `/home/robo-worker/secrets/claude-oauth-token`.
2. Make one token per caller: `uv run python3 -c 'import secrets; print(secrets.token_urlsafe(32))'`.
3. Write `/home/robo-worker/secrets/callers.toml`:

       [callers]
       agent = "<token for the agent>"
       mac = "<token for ranger's Mac>"

4. `chmod 700 /home/robo-worker/secrets && chmod 600 /home/robo-worker/secrets/*`, owned by `robo-worker`.

## Service

1. As `robo-worker`: `gh repo clone phaedrus1992/robo-worker ~/robo-worker && cd ~/robo-worker && uv sync --frozen`.
2. Copy `config.example.toml` to `/home/robo-worker/config.toml`. Set the LAN address, the ports, and `auth.oauth_token_created`.
3. `sudo cp deploy/robo-worker.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now robo-worker`.
4. Let ranger's SSH user run the two deploy commands without a password, in `/etc/sudoers.d/robo-worker`:

       ranger ALL=(robo-worker) NOPASSWD: /usr/bin/bash
       ranger ALL=(root) NOPASSWD: /usr/bin/systemctl restart robo-worker

5. Check: `curl -fsS http://<lan-address>:<port>/health` prints `"ok": true`.

## Callers

- ranger's Mac: `claude mcp add --transport http robo-worker http://<lan-address>:<port>/mcp --header "Authorization: Bearer <mac token>"`.
- The agent: part B of epic #2 adds the MCP client config.
