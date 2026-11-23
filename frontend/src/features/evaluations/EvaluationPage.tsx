import { useEffect, useMemo, useRef, useState } from "react";
import {
  Check,
  FileCode2,
  FileJson2,
  Play,
  PlugZap,
  Upload,
} from "lucide-react";
import { apiJSON } from "../../api/client";
import { sourceDigest } from "../../api/reports";
import type {
  FixtureFile,
  JobStatus,
  JobSubmission,
} from "../../api/generated/jobs";
import { navigate } from "../../app/navigation";
import { profileNames, explain } from "../../lib/presentation";
import { useService } from "./ServiceProvider";
import { inputProblems, requiredPaths } from "./inputChecks";

async function readText(file: File) {
  if (file.size > 1024 * 1024)
    throw new Error("Text files must be no larger than 1 MiB.");
  return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
    await file.arrayBuffer(),
  );
}
async function base64(file: File) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let value = "";
  for (let i = 0; i < bytes.length; i += 8192)
    value += String.fromCharCode(...bytes.subarray(i, i + 8192));
  return btoa(value);
}
export default function EvaluationPage({
  onConnect,
}: {
  onConnect: () => void;
}) {
  const service = useService();
  const [step, setStep] = useState(0);
  const [artifact, setArtifact] = useState<{
    filename: string;
    source: string;
    digest: string;
  } | null>(null);
  const [profile, setProfile] =
    useState<NonNullable<JobSubmission["profile"]>>("static");
  const [contract, setContract] = useState<string | null>(null);
  const [binding, setBinding] = useState<string | null>(null);
  const [files, setFiles] = useState<FixtureFile[]>([]);
  const [timeout, setTimeoutValue] = useState(60);
  const [consent, setConsent] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [reading, setReading] = useState(0);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const fileVersion = useRef(0);
  const inputVersion = useRef(0);
  const documentVersion = useRef({ contract: 0, binding: 0 });
  const native = !["static", "python"].includes(profile);
  const readiness = service.profiles.find((item) => item.name === profile);
  const connected =
    service.status === "connected" && service.capabilities?.execution === true;
  const sourceReady = artifact !== null;
  const problems = useMemo(
    () => inputProblems({ profile, artifact, contract, binding, files }),
    [profile, artifact, contract, binding, files],
  );
  const inputsReady =
    !native || Boolean(contract && binding && !problems.length);
  async function script(file?: File) {
    if (!file) return;
    const version = ++fileVersion.current;
    setReading((value) => value + 1);
    setArtifact(null);
    setConsent(false);
    setError("");
    try {
      if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\.py$/.test(file.name))
        throw new Error(
          "Select a Python filename with letters, numbers, underscores or hyphens, ending in .py.",
        );
      const source = await readText(file);
      if (!source || source.includes("\0"))
        throw new Error("The script must contain valid Python text.");
      const digest = await sourceDigest(await file.arrayBuffer());
      if (version === fileVersion.current)
        setArtifact({ filename: file.name, source, digest });
    } catch (cause) {
      if (version === fileVersion.current)
        setError(
          cause instanceof Error
            ? cause.message
            : "Could not read this script.",
        );
    } finally {
      setReading((value) => value - 1);
    }
  }
  async function fixtures(selected: File[]) {
    const version = ++inputVersion.current;
    setReading((value) => value + 1);
    setFiles([]);
    setConsent(false);
    setError("");
    try {
      if (
        selected.length > 16 ||
        selected.reduce((total, file) => total + file.size, 0) > 6 * 1024 * 1024
      )
        throw new Error(
          "Select at most 16 fixture/reference files, up to 6 MiB combined.",
        );
      let expected: string[] = [];
      if (contract) {
        expected = requiredPaths(contract);
      }
      const next = await Promise.all(
        selected.map(async (file) => {
          const matches = expected.filter(
            (path) => path.split("/").at(-1) === file.name,
          );
          return {
            path: matches.length === 1 ? matches[0] : file.name,
            content_base64: await base64(file),
          };
        }),
      );
      if (version === inputVersion.current) setFiles(next);
    } catch (cause) {
      if (version === inputVersion.current)
        setError(
          cause instanceof Error
            ? cause.message
            : "Could not read fixture files.",
        );
    } finally {
      setReading((value) => value - 1);
    }
  }
  async function document(
    file: File | undefined,
    kind: "contract" | "binding",
  ) {
    if (!file) return;
    const version = ++documentVersion.current[kind];
    setReading((value) => value + 1);
    setConsent(false);
    setError("");
    if (kind === "contract") {
      setContract(null);
      setFiles([]);
      inputVersion.current++;
    } else setBinding(null);
    try {
      const value = await readText(file);
      const parsed = JSON.parse(value);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        throw new Error("Expected a JSON object.");
      if (version !== documentVersion.current[kind]) return;
      if (kind === "contract") {
        setContract(value);
        setFiles([]);
      } else setBinding(value);
    } catch (cause) {
      if (version === documentVersion.current[kind])
        setError(cause instanceof Error ? cause.message : "Invalid JSON file.");
    } finally {
      setReading((value) => value - 1);
    }
  }
  async function submit() {
    if (
      !artifact ||
      !connected ||
      !readiness?.ready ||
      !consent ||
      busy ||
      reading ||
      !inputsReady
    )
      return;
    setBusy(true);
    setError("");
    try {
      const job = await apiJSON<JobStatus>("/api/v1/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filename: artifact.filename,
          source: artifact.source,
          profile,
          timeout_s: timeout,
          contract: native ? contract : null,
          binding: native ? binding : null,
          files: native ? files : [],
        }),
      });
      if (mounted.current) setConsent(false);
      void service.refresh().catch(() => {});
      if (mounted.current) navigate(`job/${job.id}`);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "The evaluation was not queued.",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="page-content evaluation-page">
      <div className="page-heading">
        <div>
          <span className="eyebrow">New run</span>
          <h2>New evaluation</h2>
        </div>
        <button className="button" onClick={() => navigate("runs")}>
          Cancel
        </button>
      </div>
      <ol className="evaluation-steps" aria-label="Evaluation steps">
        {["Script", "Inputs", "Settings", "Review"].map((label, index) => (
          <li
            key={label}
            className={
              step === index ? "current" : index < step ? "complete" : ""
            }
          >
            <button
              disabled={index > step || busy || reading > 0}
              aria-current={step === index ? "step" : undefined}
              onClick={() => setStep(index)}
            >
              <span>{index < step ? <Check /> : index + 1}</span>
              {label}
            </button>
          </li>
        ))}
      </ol>
      <div className="evaluation-layout">
        <form
          className="evaluation-body"
          onSubmit={(event) => {
            event.preventDefault();
            if (
              busy ||
              reading ||
              (step === 0 && !sourceReady) ||
              (step === 1 && !inputsReady) ||
              (step === 2 &&
                (!Number.isInteger(timeout) || timeout < 5 || timeout > 120))
            )
              return;
            if (step === 3) void submit();
            else {
              setStep(step + 1);
              setError("");
            }
          }}
        >
          {error && (
            <p className="import-error" role="alert">
              {error}
            </p>
          )}
          {step === 0 && (
            <>
              <div className="section-heading">
                <h3>Script and execution profile</h3>
                <FileCode2 />
              </div>
              <label className="file-drop script-drop">
                <Upload />
                <strong>
                  {artifact?.filename ?? "Select a Python script"}
                </strong>
                <input
                  aria-label="Evaluation script"
                  type="file"
                  accept=".py"
                  onChange={(event) => void script(event.target.files?.[0])}
                />
              </label>
              <label className="field">
                Execution profile
                <select
                  aria-label="Execution profile"
                  value={profile}
                  onChange={(event) => {
                    setProfile(
                      event.target.value as NonNullable<
                        JobSubmission["profile"]
                      >,
                    );
                    setConsent(false);
                  }}
                >
                  {Object.entries(profileNames).map(([name, label]) => (
                    <option
                      key={name}
                      value={name}
                      disabled={Boolean(
                        service.capabilities &&
                        !service.profiles.some(
                          (item) => item.name === name && item.ready,
                        ),
                      )}
                    >
                      {label}
                      {service.capabilities &&
                      !service.profiles.some(
                        (item) => item.name === name && item.ready,
                      )
                        ? " (not ready)"
                        : ""}
                    </option>
                  ))}
                </select>
              </label>
              {artifact && (
                <details className="source-preview">
                  <summary>Source preview</summary>
                  <textarea
                    aria-label="Uploaded source preview"
                    value={artifact.source}
                    readOnly
                    spellCheck={false}
                  />
                </details>
              )}
            </>
          )}
          {step === 1 && (
            <>
              <div className="section-heading">
                <h3>
                  {native ? "Workload intent and inputs" : "Input requirements"}
                </h3>
                <FileJson2 />
              </div>
              {native ? (
                <>
                  <div className="evaluation-fields">
                    <label className="field">
                      Workload contract (v2)
                      <input
                        aria-label="Workload contract"
                        type="file"
                        accept=".json"
                        onChange={(event) =>
                          void document(event.target.files?.[0], "contract")
                        }
                      />
                      {contract && (
                        <span className="good">Contract loaded</span>
                      )}
                    </label>
                    <label className="field">
                      Artifact binding
                      <input
                        aria-label="Artifact binding"
                        type="file"
                        accept=".json"
                        onChange={(event) =>
                          void document(event.target.files?.[0], "binding")
                        }
                      />
                      {binding && <span className="good">Binding loaded</span>}
                    </label>
                  </div>
                  <label className="field">
                    Fixture and reference files
                    <input
                      aria-label="Fixture and reference files"
                      type="file"
                      multiple
                      accept=".csv,.json,.txt"
                      onChange={(event) =>
                        void fixtures(Array.from(event.target.files ?? []))
                      }
                    />
                  </label>
                  {files.map((file, index) => (
                    <label className="field" key={index}>
                      Contract-relative path
                      <input
                        aria-label={`Fixture path ${index + 1}`}
                        value={file.path}
                        onChange={(event) => {
                          setConsent(false);
                          setFiles((previous) =>
                            previous.map((item, i) =>
                              i === index
                                ? { ...item, path: event.target.value }
                                : item,
                            ),
                          );
                        }}
                      />
                    </label>
                  ))}
                  <p className="notice">
                    The contract, binding and files must match this script's
                    exact bytes. At most 16 files and 6 MiB decoded data; the
                    complete request must fit 8 MiB.
                  </p>
                  {problems.length > 0 && (
                    <ul className="input-problems" role="alert">
                      {problems.map((problem) => (
                        <li key={problem}>{problem}</li>
                      ))}
                    </ul>
                  )}
                </>
              ) : (
                <div className="input-requirements">
                  <Check />
                  <div>
                    <strong>
                      {profile === "static"
                        ? "No runtime inputs required"
                        : "Self-contained script required"}
                    </strong>
                    <p>
                      {profile === "static"
                        ? "The script will be analyzed, not executed."
                        : "This profile has no data-file uploads, network access or dependency installation. Outputs are not contract-verified."}
                    </p>
                  </div>
                </div>
              )}
            </>
          )}
          {step === 2 && (
            <>
              <h3>Execution settings</h3>
              <label className="field timeout-field">
                Timeout (seconds)
                <input
                  aria-label="Execution timeout"
                  type="number"
                  min={5}
                  max={120}
                  step={5}
                  value={timeout}
                  onChange={(event) => {
                    setTimeoutValue(Number(event.target.value));
                    setConsent(false);
                  }}
                />
              </label>
              <details className="technical-details">
                <summary>Sandbox limits</summary>
                <dl className="provenance">
                  <dt>CPU quota</dt>
                  <dd>2 cores</dd>
                  <dt>Memory limit</dt>
                  <dd>2 GiB, no swap</dd>
                  <dt>Processes</dt>
                  <dd>128</dd>
                  <dt>Scratch space</dt>
                  <dd>256 MiB</dd>
                  <dt>Network</dt>
                  <dd>Disabled</dd>
                  <dt>Environment</dt>
                  <dd>
                    Approved local image, non-root, read-only root filesystem
                  </dd>
                </dl>
              </details>
            </>
          )}
          {step === 3 && (
            <>
              <h3>Review evaluation</h3>
              <dl className="review-list">
                <div>
                  <dt>Script</dt>
                  <dd>{artifact?.filename}</dd>
                </div>
                <div>
                  <dt>Profile</dt>
                  <dd>{profileNames[profile]}</dd>
                </div>
                <div>
                  <dt>Timeout</dt>
                  <dd>{timeout} seconds</dd>
                </div>
                <div>
                  <dt>Inputs</dt>
                  <dd>
                    {native
                      ? `${files.length} fixture/reference files`
                      : "No uploaded data files"}
                  </dd>
                </div>
                <div>
                  <dt>Expected evidence</dt>
                  <dd>
                    {profile === "static"
                      ? "Static analysis; no execution telemetry"
                      : native
                        ? "Native task states, contract output checks and scoped runtime telemetry"
                        : "Script exit status and scoped telemetry; outputs not verified"}
                  </dd>
                </div>
              </dl>
              <details className="technical-details">
                <summary>Artifact and image identity</summary>
                <dl className="provenance">
                  <dt>Artifact SHA-256</dt>
                  <dd>
                    <code>{artifact?.digest}</code>
                  </dd>
                  <dt>Image ID</dt>
                  <dd>
                    <code>{readiness?.image_id ?? "Not established"}</code>
                  </dd>
                </dl>
              </details>
              <p className="privacy-note">
                Source, inputs and reports are retained by this deployment until
                their job is deleted. Only submit approved workloads.
              </p>
              <label className="execution-consent">
                <input
                  type="checkbox"
                  checked={consent}
                  onChange={(event) => setConsent(event.target.checked)}
                />
                {profile === "static"
                  ? "I authorize static evaluation of this script."
                  : "I authorize executing this script in the selected isolated profile."}
              </label>
            </>
          )}
          <div className="wizard-actions">
            {step > 0 && (
              <button
                type="button"
                className="button"
                disabled={busy || reading > 0}
                onClick={() => setStep(step - 1)}
              >
                Back
              </button>
            )}
            <span className="spacer" />
            {step < 3 ? (
              <button
                className="button primary"
                type="submit"
                disabled={
                  reading > 0 ||
                  busy ||
                  (step === 0 && !sourceReady) ||
                  (step === 1 && !inputsReady) ||
                  (step === 2 &&
                    (!Number.isInteger(timeout) ||
                      timeout < 5 ||
                      timeout > 120))
                }
              >
                Continue
              </button>
            ) : (
              <button
                className="button primary"
                type="submit"
                disabled={
                  !connected ||
                  !readiness?.ready ||
                  !sourceReady ||
                  !inputsReady ||
                  !consent ||
                  busy ||
                  reading > 0
                }
              >
                <Play />
                {busy ? "Submitting..." : "Queue evaluation"}
              </button>
            )}
          </div>
        </form>
        <aside className="request-summary">
          <h3>Request summary</h3>
          <dl>
            <dt>Script</dt>
            <dd>{artifact?.filename ?? "Not selected"}</dd>
            <dt>Profile</dt>
            <dd>{profileNames[profile]}</dd>
            <dt>Deployment</dt>
            <dd>
              {!service.capabilities
                ? "Not connected"
                : service.capabilities.execution
                  ? "Execution service"
                  : "Report-only API"}
            </dd>
            <dt>Profile readiness</dt>
            <dd>
              {!service.capabilities
                ? "Not checked"
                : readiness?.ready
                  ? "Ready"
                  : (explain(readiness?.reason ?? "") ??
                    "Worker or approved profile is not ready.")}
            </dd>
          </dl>
          {!service.capabilities && (
            <button className="button" onClick={onConnect}>
              <PlugZap />
              Connect deployment
            </button>
          )}
          {service.capabilities && !service.capabilities.execution && (
            <p className="notice">Execution is disabled on this API.</p>
          )}
        </aside>
      </div>
    </div>
  );
}
