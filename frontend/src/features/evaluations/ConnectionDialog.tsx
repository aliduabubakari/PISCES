import { useRef, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { PlugZap, Unplug, X } from "lucide-react";
import { IconButton } from "../../components/ui";
import { useService } from "./ServiceProvider";

export function ConnectionDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (value: boolean) => void;
}) {
  const service = useService();
  const [token, setToken] = useState("");
  const input = useRef<HTMLInputElement>(null);
  const content = useRef<HTMLDivElement>(null);
  const connected = service.capabilities !== null;
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content
          ref={content}
          className="dialog-content"
          onOpenAutoFocus={(event) => {
            event.preventDefault();
            (input.current ?? content.current)?.focus();
          }}
        >
          <div className="dialog-heading">
            <Dialog.Title>Deployment connection</Dialog.Title>
            <Dialog.Close asChild>
              <IconButton label="Close connection">
                <X />
              </IconButton>
            </Dialog.Close>
          </div>
          <Dialog.Description className="dialog-description">
            Credentials stay in this browser session's memory. A connection does
            not submit or cancel evaluations.
          </Dialog.Description>
          <div className="connection-detail">
            <span>API</span>
            <code>Same-origin /api/v1</code>
            <span>Status</span>
            <strong>
              {connected
                ? service.capabilities?.execution
                  ? "Connected to execution service"
                  : "Connected to report-only API"
                : "Not connected"}
            </strong>
          </div>
          {service.error && (
            <p role="alert" className="import-error">
              {service.error}
            </p>
          )}
          {connected ? (
            <div className="dialog-actions">
              <button
                className="button"
                onClick={() => {
                  service.disconnect();
                  setToken("");
                }}
              >
                <Unplug />
                Disconnect
              </button>
              <button
                className="button primary"
                onClick={() => onOpenChange(false)}
              >
                Done
              </button>
            </div>
          ) : (
            <form
              onSubmit={async (event) => {
                event.preventDefault();
                try {
                  await service.connect(token);
                  setToken("");
                  onOpenChange(false);
                } catch {
                  setToken("");
                }
              }}
            >
              <label className="field">
                API token
                <input
                  ref={input}
                  aria-label="API token"
                  type="password"
                  autoComplete="off"
                  value={token}
                  onChange={(event) => setToken(event.target.value)}
                />
              </label>
              <div className="dialog-actions">
                <button
                  className="button primary"
                  disabled={service.status === "connecting"}
                  type="submit"
                >
                  <PlugZap />
                  {service.status === "connecting"
                    ? "Connecting..."
                    : "Connect API"}
                </button>
              </div>
            </form>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
