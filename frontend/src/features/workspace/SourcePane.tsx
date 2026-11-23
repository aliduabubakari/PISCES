import { useRef, useState, type ReactNode } from "react";
import Prism from "prismjs";
import "prismjs/components/prism-python";
import {
  Code2,
  Copy,
  Download,
  FileCode2,
  Link2,
  Search,
  Trash2,
  Upload,
} from "lucide-react";
import type { ReportView } from "../../api/generated/report-view";
import { download, sourceDigest } from "../../api/reports";
import { IconButton, Status } from "../../components/ui";

export type SourceAttachment = {
  name: string;
  text: string;
  digest: string;
  bytes: ArrayBuffer;
};
function tokens(values: (string | Prism.Token)[]): ReactNode[] {
  return values.map((value, index) =>
    typeof value === "string" ? (
      value
    ) : (
      <span key={index} className={`token ${value.type}`}>
        {typeof value.content === "string"
          ? value.content
          : tokens(
              Array.isArray(value.content) ? value.content : [value.content],
            )}
      </span>
    ),
  );
}

export function SourcePane({
  view,
  source,
  onAttach,
  onRemove,
}: {
  view: ReportView;
  source?: SourceAttachment;
  onAttach: (source: SourceAttachment) => void;
  onRemove: () => void;
}) {
  const fileInput = useRef<HTMLInputElement>(null);
  const code = useRef<HTMLOListElement>(null);
  const [wrap, setWrap] = useState(true);
  const [query, setQuery] = useState("");
  const [error, setError] = useState("");
  const association = !source
    ? "not attached"
    : !view.identity.artifact_sha256
      ? "no report hash"
      : source.digest === view.identity.artifact_sha256
        ? "matched"
        : "mismatched";
  const lines = source?.text.split("\n") ?? [];
  const matches = query
    ? lines.flatMap((line, index) =>
        line.toLowerCase().includes(query.toLowerCase()) ? [index] : [],
      )
    : [];
  async function attach(file?: File) {
    if (!file) return;
    if (file.size > 2 * 1024 * 1024) {
      setError("Source exceeds the 2 MiB limit.");
      return;
    }
    try {
      const bytes = await file.arrayBuffer();
      const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      onAttach({
        name: file.name,
        text,
        bytes,
        digest: await sourceDigest(bytes),
      });
      setError("");
    } catch {
      setError("Source must be a UTF-8 text file.");
    }
  }
  return (
    <section className="source-pane" aria-label="Source inspector">
      <div className="source-inspector-heading">
        <Code2 />
        Source
      </div>
      <div className="source-content">
        <div className="source-toolbar">
          <span className="filename">
            <FileCode2 />
            {source?.name ?? "No source attached"}
          </span>
          <span className="spacer" />
          <label className="wrap-control">
            <input
              type="checkbox"
              checked={wrap}
              onChange={(event) => setWrap(event.target.checked)}
            />
            Wrap
          </label>
          <IconButton
            label="Attach source"
            onClick={() => fileInput.current?.click()}
          >
            <Upload />
          </IconButton>
          <IconButton
            label="Copy source"
            disabled={!source}
            onClick={() => {
              navigator.clipboard
                .writeText(source!.text)
                .catch(() => setError("Clipboard unavailable."));
            }}
          >
            <Copy />
          </IconButton>
          <IconButton
            label="Download source"
            disabled={!source}
            onClick={() =>
              download(source!.name, new Blob([source!.bytes]), "text/plain")
            }
          >
            <Download />
          </IconButton>
          {source && (
            <IconButton label="Remove attached source" onClick={onRemove}>
              <Trash2 />
            </IconButton>
          )}
          <input
            ref={fileInput}
            className="visually-hidden"
            type="file"
            accept=".py,.json,.yaml,.yml,.txt"
            aria-label="Source file"
            onChange={(event) => {
              void attach(event.target.files?.[0]);
              event.target.value = "";
            }}
          />
        </div>
        {source ? (
          <>
            <form
              className="source-find"
              onSubmit={(event) => {
                event.preventDefault();
                if (matches.length)
                  code.current?.children[matches[0]].scrollIntoView({
                    block: "center",
                  });
              }}
            >
              <Search />
              <input
                aria-label="Find in source"
                type="search"
                placeholder="Find in source"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
              {query && <span>{matches.length} matches</span>}
              <button type="submit" className="find-button">
                Find
              </button>
            </form>
            <ol
              ref={code}
              className={`source-code ${wrap ? "wrap" : ""}`}
              aria-label="Read-only source"
            >
              {lines.map((line, index) => (
                <li
                  key={index}
                  className={matches.includes(index) ? "source-match" : ""}
                >
                  <code>
                    {tokens(
                      Prism.tokenize(line || " ", Prism.languages.python),
                    )}
                  </code>
                </li>
              ))}
            </ol>
          </>
        ) : (
          <div className="source-empty">
            <FileCode2 />
            <h3>Source not attached</h3>
            <span className="muted">
              {view.identity.artifact_sha256?.slice(0, 16) ??
                "No artifact digest"}
              {view.identity.artifact_sha256 && "..."}
            </span>
            <button
              className="button"
              type="button"
              onClick={() => fileInput.current?.click()}
            >
              <Upload />
              Attach source
            </button>
          </div>
        )}
      </div>
      <div className="source-association">
        <Link2 />
        <span>Source association</span>
        <Status value={association} />
      </div>
      {error && (
        <p role="alert" className="source-error">
          {error}
        </p>
      )}
    </section>
  );
}
