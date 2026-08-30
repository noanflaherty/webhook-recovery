// The Railway topology: one repo -> one image -> three services differing only
// by start command, sharing the Postgres plugin's injected DATABASE_URL.
//
// Infrastructure-as-Code rather than the deprecated railway.json, because it is
// the only form the CLI can apply: a root railway.json applies to *every*
// service in the project, so it cannot give the conductor and the worker
// different start commands, and the per-service config-file path is a
// dashboard-only setting.
//
//   railway config plan     # preview
//   railway config apply    # apply
//   railway up --service api --detach
//
// The builder is not declared here -- Railway detects the root Dockerfile on
// its own, and the IaC DSL has no builder/dockerfilePath option.

import { defineRailway, postgres, preserve, project, service, volume } from "railway/iac";

// Match the api's SIGTERM-to-SIGKILL buffer to the runner's drain: an
// iteration that has started is allowed to finish, so a redeploy strands no
// in_flight rows. (Lease reaping is out of scope -- see TECHNICAL_DESIGN.md
// §Leases -- which is exactly why the graceful path has to be graceful.)
const DRAIN = { RAILWAY_DEPLOYMENT_DRAINING_SECONDS: "20" };

export default defineRailway(() => {
  const Postgres = postgres("Postgres", { region: "sfo" });
  const postgresVolume = volume("postgres-volume", {
    alerts: { usage: { "80": {}, "95": {}, "100": {} } },
    allowOnlineResize: true,
    region: "sfo",
    sizeMB: 500,
  });

  // Migrations run exactly once, from one place. Three services racing
  // `alembic upgrade head` on boot can deadlock, so only the api -- which is
  // pinned to a single replica -- runs them, and it does so before it serves.
  //
  // The plan called for a Railway pre-deploy command; the IaC DSL has no
  // preDeployCommand, and putting it in the deprecated railway.json would apply
  // it to all three services and reintroduce the race. Same guarantee, and it
  // has the side benefit of being platform-independent.
  const api = service("api", {
    // PORT is declared below rather than assumed: Railway does not inject one
    // on its own, and `--port $PORT` against an unset variable expands to an
    // empty argument, so uvicorn never binds and the healthcheck times out
    // with nothing in the logs to say why. The `:-8000` fallback keeps the
    // same command working anywhere else the image runs.
    start: "alembic upgrade head && uvicorn app.api.main:app --host 0.0.0.0 --port ${PORT:-8000}",
    healthcheck: "/api/health",
    healthcheckTimeout: 60,
    replicas: 1,
    env: { PORT: "8000", DATABASE_URL: preserve(), ...DRAIN },
  });

  // One conductor. Leader election lands in Phase 1, at which point this goes
  // to 2 -- one leads, one stands by on the advisory lock. Two idle conductors
  // prove nothing today.
  const conductor = service("conductor", {
    start: "python -m app.conductor",
    replicas: 1,
    env: { DATABASE_URL: preserve(), ...DRAIN },
  });

  // Stateless and interchangeable, so scaling is a replica count and nothing
  // else. Claim contention is handled by SKIP LOCKED (Phase 1).
  const worker = service("worker", {
    start: "python -m app.worker",
    replicas: 3,
    env: { DATABASE_URL: preserve(), ...DRAIN },
  });

  return project("webhook-recovery", {
    resources: [Postgres, postgresVolume, api, conductor, worker],
  });
});
