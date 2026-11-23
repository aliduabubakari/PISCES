import type { ComponentProps, ReactNode } from "react";
import * as Tooltip from "@radix-ui/react-tooltip";

export function IconButton({
  label,
  children,
  ...props
}: ComponentProps<"button"> & { label: string; children: ReactNode }) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>
        <button
          type="button"
          className="icon-button"
          aria-label={label}
          {...props}
        >
          {children}
        </button>
      </Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content className="tooltip" sideOffset={6}>
          {label}
          <Tooltip.Arrow />
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}

export function Status({ value }: { value: string }) {
  const tone = [
    "passed",
    "completed",
    "executed",
    "available",
    "matched",
  ].includes(value)
    ? "good"
    : ["failed", "rejected", "invalid", "mismatched"].includes(value)
      ? "bad"
      : "neutral";
  return <span className={`status ${tone}`}>{value.replaceAll("_", " ")}</span>;
}
