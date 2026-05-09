/**
 * Minimal react-router-dom shim — SAM hosts owns routing, but the
 * legacy supplier pages use `useSearchParams`, `useNavigate`, and
 * `Link`. This shim provides drop-in replacements:
 *
 *   - useSearchParams → reads window.location.search
 *   - useNavigate → fires a 'sam:navigate' CustomEvent the host listens
 *     on; falls back to history.pushState if no listener is wired
 *   - Link → renders an <a> with onClick that calls navigate()
 *
 * Keeps the supplier-page diff minimal. SAM AppShell can intercept
 * sam:navigate to do real routing or ignore it for inert pages.
 */
import React, { type ReactNode } from 'react';

type NavigateFn = (
  path: string | number,
  options?: { replace?: boolean; state?: unknown },
) => void;

export function useSearchParams(): [
  URLSearchParams,
  (params: URLSearchParams | Record<string, string>) => void,
] {
  const params = new URLSearchParams(
    typeof window !== 'undefined' ? window.location.search : '',
  );
  const setParams = (next: URLSearchParams | Record<string, string>) => {
    if (typeof window === 'undefined') return;
    const sp =
      next instanceof URLSearchParams ? next : new URLSearchParams(next);
    const url = `${window.location.pathname}?${sp.toString()}`;
    window.history.replaceState(null, '', url);
  };
  return [params, setParams];
}

export function useNavigate(): NavigateFn {
  return (path, _options) => {
    if (typeof window === 'undefined') return;
    if (typeof path === 'number') {
      window.history.go(path);
      return;
    }
    window.dispatchEvent(
      new CustomEvent('sam:navigate', { detail: { path } }),
    );
    // Fallback: push the path so the URL bar reflects it even if
    // nothing listens. SAM's host can intercept and prevent default.
    try {
      window.history.pushState(null, '', path);
    } catch {
      // ignore — some hosts disallow pushState
    }
  };
}

export function Link({
  to,
  children,
  className,
  ...rest
}: {
  to: string;
  children?: ReactNode;
  className?: string;
} & Omit<React.AnchorHTMLAttributes<HTMLAnchorElement>, 'href'>): JSX.Element {
  const navigate = useNavigate();
  return (
    <a
      href={to}
      className={className}
      onClick={(e) => {
        e.preventDefault();
        navigate(to);
      }}
      {...rest}
    >
      {children}
    </a>
  );
}
