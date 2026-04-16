/**
 * libhydrogen hash (Gimli-based) implementation in JavaScript.
 * Produces hashes identical to the ESP32 libhydrogen library.
 *
 * Constants:
 *   gimli_BLOCKBYTES = 48, gimli_RATE = 16, gimli_DOMAIN_XOF = 0x0f
 *   hydro_hash_BYTES = 32, hydro_hash_CONTEXTBYTES = 8
 */

const BLOCKBYTES = 48;
const RATE = 16;
const DOMAIN_XOF = 0x0f;
const HASH_BYTES = 32;

/* ── Gimli permutation (portable, 24 rounds) ── */
function rotl32(x, b) {
  return ((x << b) | (x >>> (32 - b))) >>> 0;
}

function gimliCore(state) {
  // state is a Uint32Array[12]
  for (let round = 24; round > 0; round--) {
    for (let col = 0; col < 4; col++) {
      const x = rotl32(state[col], 24);
      const y = rotl32(state[4 + col], 9);
      const z = state[8 + col];

      state[8 + col] = (x ^ (z << 1) ^ ((y & z) << 2)) >>> 0;
      state[4 + col] = (y ^ x ^ ((x | z) << 1)) >>> 0;
      state[col]     = (z ^ y ^ ((x & y) << 3)) >>> 0;
    }
    switch (round & 3) {
      case 0: {
        let tmp = state[0]; state[0] = state[1]; state[1] = tmp;
        tmp = state[2]; state[2] = state[3]; state[3] = tmp;
        state[0] = (state[0] ^ (0x9e377900 | round)) >>> 0;
        break;
      }
      case 2: {
        let tmp = state[0]; state[0] = state[2]; state[2] = tmp;
        tmp = state[1]; state[1] = state[3]; state[3] = tmp;
        break;
      }
    }
  }
}

/* ── Helpers operating on the u8 state view ── */
function gimliCoreU8(buf, tag) {
  // buf is a Uint8Array[48]
  buf[BLOCKBYTES - 1] ^= tag;
  // Convert to uint32 LE, run permutation, convert back
  const s = new Uint32Array(12);
  const dv = new DataView(buf.buffer, buf.byteOffset, BLOCKBYTES);
  for (let i = 0; i < 12; i++) s[i] = dv.getUint32(i * 4, true);
  gimliCore(s);
  for (let i = 0; i < 12; i++) dv.setUint32(i * 4, s[i], true);
}

function gimliPadU8(buf, pos, domain) {
  buf[pos] ^= (domain << 1) | 1;
  buf[RATE - 1] ^= 0x80;
}

/* ── hydro_hash streaming API ── */
class HydroHashState {
  constructor() {
    this.state = new Uint8Array(BLOCKBYTES); // 48 bytes
    this.bufOff = 0;
  }
}

function hydroHashUpdate(st, data) {
  // data is Uint8Array
  let offset = 0;
  let remaining = data.length;

  while (remaining > 0) {
    const left = RATE - st.bufOff;
    const ps = remaining > left ? left : remaining;
    for (let i = 0; i < ps; i++) {
      st.state[st.bufOff + i] ^= data[offset + i];
    }
    offset += ps;
    remaining -= ps;
    st.bufOff += ps;
    if (st.bufOff === RATE) {
      gimliCoreU8(st.state, 0);
      st.bufOff = 0;
    }
  }
}

function hydroHashInit(ctx) {
  // ctx: 8-byte string (e.g. "metering")
  // key: null (no key)
  const ctxBytes = new Uint8Array(8);
  for (let i = 0; i < 8 && i < ctx.length; i++) {
    ctxBytes[i] = ctx.charCodeAt(i);
  }

  // block = { 4, 'k', 'm', 'a', 'c', 8, ...ctx[8]..., 0-padding }
  const block = new Uint8Array(64);
  block[0] = 4;
  block[1] = 0x6b; // 'k'
  block[2] = 0x6d; // 'm'
  block[3] = 0x61; // 'a'
  block[4] = 0x63; // 'c'
  block[5] = 8;
  block.set(ctxBytes, 6);
  // bytes 14..63 are zero (already)

  // No key: block[RATE] = 0
  block[RATE] = 0;
  // p = (RATE + 1 + 0 + (RATE - 1)) & ~(RATE - 1) = (16+1+0+15) & ~15 = 32 & ~15 = 32
  const p = 32;

  const st = new HydroHashState();
  hydroHashUpdate(st, block.subarray(0, p));
  return st;
}

function hydroHashFinal(st, outLen) {
  if (outLen === undefined) outLen = HASH_BYTES;

  // Encode output length: right_enc(out_len) || 0x00
  const lc = new Uint8Array(4);
  lc[1] = outLen & 0xff;
  lc[2] = (outLen >> 8) & 0xff;
  lc[3] = 0;
  const lcLen = 1 + (lc[2] !== 0 ? 1 : 0);
  lc[0] = lcLen;
  hydroHashUpdate(st, lc.subarray(0, 1 + lcLen + 1));

  // Pad
  gimliPadU8(st.state, st.bufOff, DOMAIN_XOF);

  // Squeeze
  const out = new Uint8Array(outLen);
  const fullBlocks = Math.floor(outLen / RATE);
  for (let i = 0; i < fullBlocks; i++) {
    gimliCoreU8(st.state, 0);
    out.set(st.state.subarray(0, RATE), i * RATE);
  }
  const leftover = outLen % RATE;
  if (leftover > 0) {
    gimliCoreU8(st.state, 0);
    out.set(st.state.subarray(0, leftover), fullBlocks * RATE);
  }

  return out;
}

/* ── Convenience: hash a Buffer/Uint8Array in one shot ── */
function hydroHash(data, ctx, outLen) {
  if (typeof ctx !== 'string' || ctx.length > 8) {
    throw new Error('Context must be a string of at most 8 characters');
  }
  if (outLen === undefined) outLen = HASH_BYTES;

  const st = hydroHashInit(ctx);
  // Process in chunks to keep memory bounded
  const chunk = 4096;
  for (let off = 0; off < data.length; off += chunk) {
    const end = Math.min(off + chunk, data.length);
    hydroHashUpdate(st, data.subarray ? data.subarray(off, end) : new Uint8Array(data.buffer || data, off, end - off));
  }
  return hydroHashFinal(st, outLen);
}

function hydroHashHex(data, ctx, outLen) {
  const hash = hydroHash(data instanceof Buffer ? new Uint8Array(data) : data, ctx, outLen);
  return Buffer.from(hash).toString('hex');
}

module.exports = { hydroHash, hydroHashHex, hydroHashInit, hydroHashUpdate, hydroHashFinal };
