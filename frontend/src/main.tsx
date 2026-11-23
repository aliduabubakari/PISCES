import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./app/App";
import ErrorBoundary from "./components/ErrorBoundary";
import "@fontsource/ibm-plex-sans/latin-400.css";
import "@fontsource/ibm-plex-sans/latin-500.css";
import "@fontsource/ibm-plex-sans/latin-600.css";
import "@fontsource/ibm-plex-mono/latin-400.css";
import "./styles.css";
import "./workspace.css";
import brandMark from "../../assets/brand/pisces-mark.svg";

const favicon =
  document.querySelector<HTMLLinkElement>('link[rel="icon"]') ??
  document.createElement("link");
favicon.rel = "icon";
favicon.type = "image/svg+xml";
favicon.href = brandMark;
document.head.append(favicon);

const root =
  import.meta.hot?.data.root ?? createRoot(document.getElementById("root")!);
if (import.meta.hot) import.meta.hot.data.root = root;
root.render(
  <StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </StrictMode>,
);
