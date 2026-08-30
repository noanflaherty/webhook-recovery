// The Railway topology: one repo -> one image -> three services differing only
// by start command, sharing the Postgres plugin's injected DATABASE_URL.
//
// Infrastructure-as-Code rather than the deprecated railway.json, because it is
// the only form the CLI can apply: a root railway.json applies to *every*
// service in the project, so it cannot give the conductor and the worker
// different start commands, and the per-service config-file path is a
// dashboard-only setting.
//
//   npm install                 # the IaC SDK, needs node >= 22
//   railway config plan         # preview
//   railway config apply --yes
//   railway up --service api --detach

import { defineRailway, postgres, preserve, project, service, volume } from "railway/iac";

// Pin the Dockerfile builder. Railway re-detects a builder per service, and it
// has silently fallen back to Railpack here -- which then fails on
// `No interpreter found for Python ==3.12.*`, several minutes into a build, for
// a project that has a working Dockerfile sitting at the root. The IaC DSL has
// no `builder` option, so this variable is the way to say it from code.
const DOCKERFILE = { RAILWAY_DOCKERFILE_PATH: "Dockerfile" };

// Match the SIGTERM-to-SIGKILL buffer to the runner's drain: an iteration that
// has started is allowed to finish, so a redeploy strands no in_flight rows.
// (Lease reaping is out of scope -- see TECHNICAL_DESIGN.md §Leases -- which is
// exactly why the graceful path has to be graceful.)
const DRAIN = { RAILWAY_DEPLOYMENT_DRAINING_SECONDS: "20" };

const COMMON = { DATABASE_URL: preserve(), ...DOCKERFILE, ...DRAIN };

export default defineRailway(() => {
  const Postgres = postgres("Postgres", { region: "sfo" });
  const postgresVolume = volume("postgres-volume", {
    alerts: { usage: { "80": {}, "95": {}, "100": {} } },
    allowOnlineResize: true,
    region: "sfo",
    sizeMB: 500,
  });

  // The start command is a single script rather than
  // `alembic upgrade head && uvicorn ...`: Railway's start-command parser does
  // its own interpolation and word splitting, so shell operators and
  // `${VAR:-default}` are unreliable (the latter reaches the process as a
  // literal string, and uvicorn exits on the unparseable port). Everything that
  // needs a shell lives inside scripts/start-api.sh, where a real /bin/sh runs
  // it -- and that script is testable with `docker run`, which the start
  // command is not.
  //
  // Migrations run there, from this service only, which is pinned to one
  // replica: the same "exactly once, from one place" guarantee compose gets
  // from its one-shot migrate service. The IaC DSL has no preDeployCommand
  // (Railway's own `config migrate` comments the field out), and a root
  // railway.json would apply one to all three services and reintroduce the
  // race the one-shot step exists to prevent.
  const api = service("api", {
    start: "./scripts/start-api.sh",
    healthcheck: "/api/health",
    healthcheckTimeout: 120,
    replicas: 1,
    env: { PORT: "8000", ...COMMON },
  });

  // One conductor. Leader election lands in Phase 1, at which point this goes
  // to 2 -- one leads, one stands by on the advisory lock.
  const conductor = service("conductor", {
    start: "python -m app.conductor",
    replicas: 1,
    env: COMMON,
  });

  // The design calls for 3, and compose runs 3. Railway's free tier rejects any
  // service with more than 2 replicas outright ("Total replicas across all
  // regions must be less than or equal to 2"), so the deployed copy runs 2.
  // Workers are stateless and interchangeable, so this is a capacity
  // difference and nothing else -- claim contention is handled by SKIP LOCKED.
  const worker = service("worker", {
    start: "python -m app.worker",
    replicas: 2,
    env: COMMON,
  });

  return project("webhook-recovery", {
    resources: [Postgres, postgresVolume, api, conductor, worker],
  });
});
