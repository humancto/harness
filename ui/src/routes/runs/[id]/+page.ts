// Dynamic-param route: cannot be prerendered (root layout sets
// prerender=true for the SPA shell; adapter-static strict would fail
// the build otherwise — 4.8 plan review MAJOR-3). Served by the SPA
// fallback at runtime.
export const prerender = false;
