import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { apiJSON, setApiToken } from "../../api/client";
import type { JobStatus, ProfileReadiness } from "../../api/generated/jobs";

type Capabilities = { execution: boolean; persistence: boolean };
type Service = {
  status:
    "disconnected" | "connecting" | "connected" | "reconnecting" | "error";
  capabilities: Capabilities | null;
  jobs: JobStatus[];
  profiles: ProfileReadiness[];
  error: string;
  updatedAt: number | null;
  connect: (token: string) => Promise<void>;
  disconnect: () => void;
  refresh: () => Promise<void>;
};
const Context = createContext<Service | null>(null);
export function useService() {
  const value = useContext(Context);
  if (!value) throw new Error("ServiceProvider missing");
  return value;
}
export function ServiceProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<Service["status"]>("disconnected");
  const [capabilities, setCapabilities] = useState<Capabilities | null>(null);
  const [jobs, setJobs] = useState<JobStatus[]>([]);
  const [profiles, setProfiles] = useState<ProfileReadiness[]>([]);
  const [error, setError] = useState("");
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const generation = useRef(0);
  const load = useCallback(async (version: number) => {
    const next = await apiJSON<Capabilities>("/api/v1/capabilities");
    if (version !== generation.current) return;
    const [nextJobs, nextProfiles] = next.execution
      ? await Promise.all([
          apiJSON<JobStatus[]>("/api/v1/jobs"),
          apiJSON<ProfileReadiness[]>("/api/v1/jobs/profiles"),
        ])
      : [[], []];
    if (version !== generation.current) return;
    setCapabilities((previous) =>
      previous &&
      previous.execution === next.execution &&
      previous.persistence === next.persistence
        ? previous
        : next,
    );
    setJobs(nextJobs);
    setProfiles(nextProfiles);
    setUpdatedAt(Date.now());
    setError("");
  }, []);
  const refresh = useCallback(async () => {
    await load(generation.current);
  }, [load]);
  const disconnect = useCallback(() => {
    generation.current++;
    setApiToken("");
    setStatus("disconnected");
    setCapabilities(null);
    setJobs([]);
    setProfiles([]);
    setError("");
    setUpdatedAt(null);
  }, []);
  const connect = useCallback(
    async (token: string) => {
      const version = ++generation.current;
      setStatus("connecting");
      setError("");
      setApiToken(token);
      try {
        await load(version);
        if (version !== generation.current) return;
        setStatus("connected");
      } catch (cause) {
        if (version !== generation.current) return;
        setApiToken("");
        setCapabilities(null);
        setJobs([]);
        setProfiles([]);
        setStatus("error");
        setError(cause instanceof Error ? cause.message : "Connection failed.");
        throw cause;
      }
    },
    [load],
  );
  const pollingEnabled =
    capabilities !== null && ["connected", "reconnecting"].includes(status);
  useEffect(() => {
    if (!pollingEnabled) return;
    const version = generation.current;
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    let failures = 0;
    async function poll() {
      try {
        await load(version);
        if (active && version === generation.current) {
          failures = 0;
          setStatus("connected");
        }
      } catch {
        if (active && version === generation.current) {
          failures++;
          setStatus("reconnecting");
          setError(
            "Connection interrupted. Showing the last received job states.",
          );
        }
      } finally {
        if (active && version === generation.current)
          timer = setTimeout(poll, Math.min(15000, 2000 * 2 ** failures));
      }
    }
    timer = setTimeout(poll, 2000);
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [pollingEnabled, load]);
  return (
    <Context.Provider
      value={{
        status,
        capabilities,
        jobs,
        profiles,
        error,
        updatedAt,
        connect,
        disconnect,
        refresh,
      }}
    >
      {children}
    </Context.Provider>
  );
}
