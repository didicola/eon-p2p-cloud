/* EON noise module — ported from cosmos-space (ancestor repo) for eon-3d scenes.
 * Sealed D2572. Dependency-free: hash/lerp/smoothstep/noise2D/fbm/clamp.
 * Usage: const n = EONNoise.fbm(u*scale+seed, v*scale+seed*1.3, 5); */
(function (root) {
  'use strict';
  function hash(x, y) {
    let h = x * 374761393 + y * 668265263;
    h = (h ^ (h >> 13)) * 1274126177;
    return (h ^ (h >> 16)) & 0x7fffffff;
  }
  function lerp(a, b, t) { return a + (b - a) * t; }
  function smoothstep(t) { return t * t * (3 - 2 * t); }
  function noise2D(x, y) {
    const ix = Math.floor(x), iy = Math.floor(y);
    const fx = x - ix, fy = y - iy;
    const sx = smoothstep(fx), sy = smoothstep(fy);
    const n00 = hash(ix, iy) / 0x7fffffff;
    const n10 = hash(ix + 1, iy) / 0x7fffffff;
    const n01 = hash(ix, iy + 1) / 0x7fffffff;
    const n11 = hash(ix + 1, iy + 1) / 0x7fffffff;
    return lerp(lerp(n00, n10, sx), lerp(n01, n11, sx), sy);
  }
  function fbm(x, y, octaves) {
    octaves = octaves || 5;
    let val = 0, amp = 0.5, freq = 1;
    for (let i = 0; i < octaves; i++) {
      val += amp * noise2D(x * freq, y * freq);
      amp *= 0.5; freq *= 2;
    }
    return val;
  }
  function clamp(v, mn, mx) { return Math.max(mn, Math.min(mx, v)); }
  root.EONNoise = { hash, lerp, smoothstep, noise2D, fbm, clamp };
})(typeof window !== 'undefined' ? window : globalThis);
