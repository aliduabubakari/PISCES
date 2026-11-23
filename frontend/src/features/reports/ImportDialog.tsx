import { useRef, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { FileJson2, FolderOpen, X } from "lucide-react";
import { IconButton } from "../../components/ui";
import { openReport, type WorkspaceEntry } from "../../api/reports";

export function ImportDialog({
  open,
  onOpenChange,
  onImport,
}: {
  open: boolean;
  onOpenChange: (value: boolean) => void;
  onImport: (entries: WorkspaceEntry[]) => void;
}) {
  const [files, setFiles] = useState<File[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  async function submit() {
    if (!files.length || busy) return;
    setBusy(true);
    setError("");
    try {
      if (files.length > 10)
        throw new Error("Open at most 10 reports at once.");
      const entries = [];
      for (const file of files) entries.push(await openReport(file));
      onImport(entries);
      onOpenChange(false);
      setFiles([]);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Could not open the selected reports.",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <Dialog.Root
      open={open}
      onOpenChange={(next) => {
        if (!busy) {
          onOpenChange(next);
          setError("");
        }
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content
          className="dialog-content"
          onOpenAutoFocus={(event) => {
            event.preventDefault();
            input.current?.focus();
          }}
        >
          <div className="dialog-heading">
            <Dialog.Title>Open reports</Dialog.Title>
            <Dialog.Close asChild>
              <IconButton label="Close import" disabled={busy}>
                <X />
              </IconButton>
            </Dialog.Close>
          </div>
          <Dialog.Description className="dialog-description">
            JSON reports and portable view bundles. Importing a report never
            executes its source.
          </Dialog.Description>
          <label className="file-drop">
            <FileJson2 />
            Report files
            <input
              type="file"
              aria-label="Report files"
              ref={input}
              accept=".json"
              multiple
              onChange={(event) => {
                setFiles(Array.from(event.target.files ?? []));
                setError("");
              }}
            />
          </label>
          {files.length > 0 && (
            <ul className="file-list">
              {files.map((file, index) => (
                <li key={index}>
                  <FileJson2 />
                  {file.name}
                  <span>{(file.size / 1024).toFixed(1)} KiB</span>
                </li>
              ))}
            </ul>
          )}
          {error && (
            <p className="import-error" role="alert">
              {error}
            </p>
          )}
          <div className="dialog-actions">
            <Dialog.Close asChild>
              <button className="button" disabled={busy}>
                Cancel
              </button>
            </Dialog.Close>
            <button
              className="button primary"
              disabled={!files.length || busy}
              onClick={() => void submit()}
            >
              <FolderOpen />
              {busy ? "Opening..." : "Open locally"}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
