// Slackwater Pump — a paper-trading bot for the make.fun pump.fun replay
// (protocol agent-fun/replay/v2).
//
// The harness talks to this bot as newline-delimited JSON on stdin/stdout:
// it opens with one `hello` line (capital, caps, fees, latency), then one
// `batch` line per slot carrying the curve events, marks, fills and account
// for that slot, and finally a `done` line. This bot answers every batch
// with exactly one `reply` line echoing the batch seq and listing its
// orders for that slot. Orders execute two slots later (latency_slots), so
// every decision here is made on data that is already two slots old.
//
// Strategy — a dip-buy inside an uptrend, adapted from the Slackwater
// pattern (buy a small dip off the recent high while the longer trend still
// points up, scale into strength, take profit early, cut losers fast):
//
//   * A curve becomes a candidate once it is mature (a few hundred slots
//     past its CreateEvent, so bundled launch dumps are behind it) and its
//     real SOL reserves sit in a liquid band.
//   * Entry needs three things at once: the price has dipped at least 1%
//     below its recent high, the price is still at least 2% above where it
//     stood a longer window ago (the uptrend is intact), and real buy flow
//     over the short window is positive with several distinct buyers.
//   * The first buy is small (0.05 SOL). If the price recovers above the
//     last buy and flow stays positive, the bot scales in, up to 0.25 SOL
//     per coin. False starts cost little; runners get size.
//   * A position is sold whole on the first of: +30% take profit, a 12%
//     trailing stop once the position has been up 15%, a 12% hard stop
//     against cost, a fade (net flow over the last slots turns strongly
//     negative), or a time limit of 1500 slots held.
//   * Guardrail pacing: the competition ranks only entries with a round
//     trip of at least 0.01 SOL on 20+ coins and a max drawdown of 50% or
//     less. When the bot's own fills or coins lag the pace implied by the
//     elapsed block time, it opens tiny probe round trips (buy 0.02 SOL of
//     a liquid curve, sell it as soon as the buy lands) until it is back
//     on pace.
//
// Everything is deterministic by construction: no clock, no randomness,
// iteration only over insertion-ordered maps fed by the batch stream, so
// two runs on the same data emit byte-identical replies.

import * as readline from 'node:readline';

const LAMPORTS_PER_SOL = 1_000_000_000;

// ---- Tunable strategy parameters (SOL unless noted, slots otherwise) ----
const ENTRY_WINDOW = 60; // slots over which the dip off the high is measured
const TREND_WINDOW = 150; // slots over which the uptrend must hold
const FLOW_WINDOW = 40; // slots over which buy flow is measured
const FADE_WINDOW = 20; // slots over which a fade exit is measured
const MIN_AGE = 400; // slots after CreateEvent before an entry is allowed
const MIN_REAL_SOL = 1; // entry only when real SOL reserves are in [1, 100]
const MAX_REAL_SOL = 100;
const MIN_NET_FLOW_SOL = 0.5; // net buy SOL over FLOW_WINDOW
const MIN_BUYS = 4; // buy trades in FLOW_WINDOW
const MIN_BUYERS = 4; // distinct buying wallets in FLOW_WINDOW
const DIP_BPS = 100; // price must sit >=1% below the ENTRY_WINDOW high
const TREND_MIN = 0.02; // price must be >=2% above the TREND_WINDOW start
const INITIAL_SOL = 0.05; // first buy of a position
const ADD_SOL = 0.1; // each scale-in buy
const MAX_STAKE_SOL = 0.25; // most SOL in one position
const MIN_BUY_SOL = 0.01; // smallest buy worth sending (guardrail floor)
const MAX_EXPOSURE_SOL = 6; // most SOL at cost across all positions
const BUY_SLIPPAGE_BPS = 1500;
const SELL_SLIPPAGE_BPS = 10_000;
const TAKE_PROFIT = 0.15; // sell whole at +15% vs cost
const TRAIL_ARM = 0.08; // trailing stop arms once up 8%
const TRAIL_DROP = 0.08; // sell once value falls 8% below its peak
const STOP_LOSS = 0.08; // sell once value falls 8% below cost
const FADE_NET_SOL = -0.3; // sell when net flow over FADE_WINDOW is below this
const MAX_HOLD_SLOTS = 900; // time limit on a position
const COOLDOWN_SLOTS = 200; // wait after an exit before re-entering a curve
const MAX_ENTRIES_PER_COIN = 3;
const PROBE_SOL = 0.02; // probe round-trip size
const TARGET_COINS = 30; // guardrail pacing targets
const TARGET_FILLS = 240;
const PACE_SECS = 14_400; // pace targets scale over this many block seconds
const MAX_PROBES = 1; // probes open at once

// ---- Small helpers ----

type Mint = string;

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface CurveTape {
  createdSlot: number | null;
  complete: boolean;
  trades: Trade[];
  entries: number;
  lastExitSlot: number;
}

interface HeldPosition {
  openedSlot: number;
  peakValueSol: number;
  investedSol: number;
  filled: boolean;
  exiting: boolean;
  lastBuyPrice: number;
  lastValueSol: number;
}

interface CurveState {
  virtualQuote: bigint;
  virtualToken: bigint;
  realQuote: bigint;
  complete: boolean;
}

function priceOf(curve: CurveState): number {
  if (curve.virtualToken === 0n) return 0;
  return Number(curve.virtualQuote) / Number(curve.virtualToken);
}

/** SOL a sale of `tokens` would return, after the 1.25% curve fee and tx fee. */
function sellValueSol(tokens: bigint, curve: CurveState, txFeeSol: number): number {
  const vt = Number(curve.virtualToken);
  const vq = Number(curve.virtualQuote);
  const t = Number(tokens);
  if (vt <= 0 || vq <= 0 || t <= 0) return 0;
  const gross = (t * vq) / (vt + t) / LAMPORTS_PER_SOL;
  return gross * (1 - 0.0125) - txFeeSol;
}

// ---- Bot state ----

const tapes = new Map<Mint, CurveTape>();
const curves = new Map<Mint, CurveState>();
const held = new Map<Mint, HeldPosition>();
const probes = new Map<Mint, boolean>(); // false = buying, true = selling
const pending = new Map<string, { mint: Mint; side: 'buy' | 'sell' }>(); // client_id -> order in flight
let myFills = 0;
const myCoins = new Set<Mint>();
let orderCounter = 0;
let firstBlockTime: number | null = null;
let lastBlockTime: number | null = null;

let capitalLamports = 10_000_000_000n;
let maxOpenPositions = 5;
let maxOrderBpsOfRealReserve = 100;
let maxOrdersPerBatch = 16;
let txFeeLamports = 105_000n; // base 5000 + priority 100000, replaced by hello

function tapeOf(mint: Mint): CurveTape {
  let tape = tapes.get(mint);
  if (tape === undefined) {
    tape = { createdSlot: null, complete: false, trades: [], entries: 0, lastExitSlot: -1e15 };
    tapes.set(mint, tape);
  }
  return tape;
}

/** Net recorded SOL (buys minus sells) over the last `window` slots. */
function netFlow(tape: CurveTape, now: number, window: number): number {
  let net = 0;
  for (let i = tape.trades.length - 1; i >= 0; i--) {
    const trade = tape.trades[i]!;
    if (trade.slot <= now - window) break;
    net += trade.buy ? trade.sol : -trade.sol;
  }
  return net;
}

/**
 * Entry signal for a curve at slot `now`: mature, liquid, dipped off its
 * recent high while the longer trend still points up, with real buy flow
 * from several distinct buyers.
 */
function entrySignal(tape: CurveTape, now: number, realSol: number): boolean {
  if (realSol < MIN_REAL_SOL || realSol > MAX_REAL_SOL) return false;
  if (tape.createdSlot === null || now - tape.createdSlot < MIN_AGE) return false;
  const from = now - TREND_WINDOW;
  let startPrice = -1;
  let high = 0;
  let last = 0;
  let buySol = 0;
  let sellSol = 0;
  let buys = 0;
  const buyers = new Set<string>();
  for (const trade of tape.trades) {
    if (trade.slot <= from) continue;
    if (startPrice < 0) startPrice = trade.price;
    if (trade.price > high) high = trade.price;
    last = trade.price;
    if (trade.slot > now - FLOW_WINDOW) {
      if (trade.buy) {
        buySol += trade.sol;
        buys++;
        buyers.add(trade.user);
      } else {
        sellSol += trade.sol;
      }
    }
  }
  if (startPrice <= 0 || last <= 0 || high <= 0) return false;
  if (buys < MIN_BUYS || buyers.size < MIN_BUYERS) return false;
  if (buySol - sellSol < MIN_NET_FLOW_SOL) return false;
  if (last / startPrice - 1 < TREND_MIN) return false;
  if (last > high * (1 - DIP_BPS / 10_000)) return false;
  return true;
}

interface Order {
  side: 'buy' | 'sell';
  client_id: string;
  mint: Mint;
  quote_lamports?: string;
  token_amount?: string;
  max_slippage_bps: number;
}

function nextClientId(): string {
  orderCounter++;
  return `o${orderCounter}`;
}

/** Decide the orders for one batch. `batch` is the parsed batch message. */
function decideOrders(batch: any): Order[] {
  const now: number = batch.slot;
  const orders: Order[] = [];
  const txFeeSol = Number(txFeeLamports) / LAMPORTS_PER_SOL;

  // Fills from earlier batches: count ours, drop them from pending.
  for (const fill of batch.fills ?? []) {
    if (typeof fill.client_id === 'string' && fill.client_id.startsWith('_')) continue;
    if (pending.has(fill.client_id)) {
      pending.delete(fill.client_id);
      if (fill.status === 'filled') {
        myFills++;
        myCoins.add(fill.mint);
      }
    }
  }

  // Fold this batch's events into the tapes and curve states.
  const traded = new Set<Mint>();
  for (const event of batch.events ?? []) {
    if (event.block_time !== null && event.block_time !== undefined) {
      if (firstBlockTime === null) firstBlockTime = event.block_time;
      if (lastBlockTime === null || event.block_time > lastBlockTime) lastBlockTime = event.block_time;
    }
    if (event.event === 'CreateEvent') {
      const tape = tapeOf(event.mint);
      tape.createdSlot = event.slot;
      curves.set(event.mint, {
        virtualQuote: BigInt(event.virtual_sol_reserves ?? event.virtual_quote_reserves ?? '0'),
        virtualToken: BigInt(event.virtual_token_reserves),
        realQuote: 0n,
        complete: false,
      });
    } else if (event.event === 'TradeEvent') {
      const tape = tapeOf(event.mint);
      const sol = Number(event.sol_amount) / LAMPORTS_PER_SOL;
      const price =
        Number(event.virtual_quote_reserves ?? event.virtual_sol_reserves) /
        Number(event.virtual_token_reserves);
      tape.trades.push({ slot: event.slot, buy: event.is_buy, sol, user: event.user, price });
      traded.add(event.mint);
      curves.set(event.mint, {
        virtualQuote: BigInt(event.virtual_quote_reserves ?? event.virtual_sol_reserves),
        virtualToken: BigInt(event.virtual_token_reserves),
        realQuote: BigInt(event.real_quote_reserves ?? event.real_sol_reserves ?? '0'),
        complete: false,
      });
    } else if (event.event === 'CompleteEvent') {
      tapeOf(event.mint).complete = true;
      const curve = curves.get(event.mint);
      if (curve) curve.complete = true;
    }
  }

  // Marks carry the freshest reserves for every curve that moved.
  const marks = new Map<Mint, CurveState>();
  for (const mark of batch.marks ?? []) {
    marks.set(mark.mint, {
      virtualQuote: BigInt(mark.virtual_quote_reserves),
      virtualToken: BigInt(mark.virtual_token_reserves),
      realQuote: BigInt(mark.real_quote_reserves),
      complete: false,
    });
  }

  // Trim tapes to the longest window we read, so memory stays bounded.
  const keepFrom = now - Math.max(TREND_WINDOW, FLOW_WINDOW, FADE_WINDOW) - 5;
  for (const mint of traded) {
    const tape = tapes.get(mint)!;
    let cut = 0;
    while (cut < tape.trades.length && tape.trades[cut]!.slot <= keepFrom) cut++;
    if (cut > 0) tape.trades.splice(0, cut);
  }

  const account = batch.account ?? { cash_lamports: '0', positions: [] };
  const positions = new Map<Mint, { token_amount: string; cost_lamports: string }>();
  for (const pos of account.positions ?? []) positions.set(pos.mint, pos);
  let freeSol = Number(BigInt(account.cash_lamports)) / LAMPORTS_PER_SOL;

  const pendingByMint = new Map<Mint, number>();
  for (const entry of pending.values()) pendingByMint.set(entry.mint, (pendingByMint.get(entry.mint) ?? 0) + 1);

  const push = (order: Order): boolean => {
    if (orders.length >= maxOrdersPerBatch) return false;
    orders.push(order);
    pending.set(order.client_id, { mint: order.mint, side: order.side });
    return true;
  };

  // ---- Exits first: sell whole positions on the first exit trigger. ----
  for (const [mint, heldPos] of held) {
    const pos = positions.get(mint);
    const curve = marks.get(mint) ?? curves.get(mint);
    if (pos === undefined) {
      if (!pendingByMint.has(mint)) {
        held.delete(mint);
        if (heldPos.filled) tapeOf(mint).lastExitSlot = now;
        else tapeOf(mint).entries--;
      }
      continue;
    }
    heldPos.filled = true;
    if (curve === undefined || curve.complete) continue;
    const value = sellValueSol(BigInt(pos.token_amount), curve, txFeeSol);
    const cost = Number(BigInt(pos.cost_lamports)) / LAMPORTS_PER_SOL;
    if (value > heldPos.peakValueSol) heldPos.peakValueSol = value;
    heldPos.lastValueSol = value;
    let exit = heldPos.exiting;
    const tape = tapeOf(mint);
    if (!exit && value >= cost * (1 + TAKE_PROFIT)) exit = true;
    if (!exit && value < cost * (1 - STOP_LOSS)) exit = true;
    if (!exit && heldPos.peakValueSol >= cost * (1 + TRAIL_ARM) && value < heldPos.peakValueSol * (1 - TRAIL_DROP)) exit = true;
    if (!exit && netFlow(tape, now, FADE_WINDOW) < FADE_NET_SOL) exit = true;
    if (!exit && now - heldPos.openedSlot > MAX_HOLD_SLOTS && value < cost * 1.05) exit = true;
    if (!exit) continue;
    heldPos.exiting = true;
    let sellPending = false;
    for (const entry of pending.values()) {
      if (entry.mint === mint && entry.side === 'sell') sellPending = true;
    }
    if (sellPending) continue;
    push({ side: 'sell', client_id: nextClientId(), mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIPPAGE_BPS });
  }

  // ---- Probes: sell a probe as soon as its buy has landed. ----
  for (const [mint, selling] of probes) {
    const pos = positions.get(mint);
    const curve = marks.get(mint) ?? curves.get(mint);
    if (pos === undefined) {
      if (!pendingByMint.has(mint)) probes.delete(mint);
      continue;
    }
    if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
    if (selling) continue;
    if (push({ side: 'sell', client_id: nextClientId(), mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIPPAGE_BPS })) {
      probes.set(mint, true);
    }
  }

  // ---- Entries and scale-ins. ----
  let slotsLeft = maxOpenPositions - held.size - probes.size;
  let exposureSol = 0;
  for (const [mint, heldPos] of held) {
    const pos = positions.get(mint);
    exposureSol += Math.max(heldPos.investedSol, pos ? Number(BigInt(pos.cost_lamports)) / LAMPORTS_PER_SOL : 0);
  }
  for (const mint of traded) {
    if (orders.length >= maxOrdersPerBatch) break;
    const curve = marks.get(mint) ?? curves.get(mint);
    if (curve === undefined || curve.complete) continue;
    if (pendingByMint.has(mint) || probes.has(mint)) continue;
    const tape = tapes.get(mint)!;
    const heldPos = held.get(mint);
    const price = priceOf(curve);
    const realSol = Number(curve.realQuote) / LAMPORTS_PER_SOL;
    let wantSol: number;
    if (heldPos === undefined) {
      if (slotsLeft <= 0) continue;
      if (tape.entries >= MAX_ENTRIES_PER_COIN || now - tape.lastExitSlot < COOLDOWN_SLOTS) continue;
      if (!entrySignal(tape, now, realSol)) continue;
      wantSol = Math.min(INITIAL_SOL, MAX_STAKE_SOL);
    } else {
      // Scale into strength only: price above the last buy, flow still positive.
      if (heldPos.exiting || !heldPos.filled) continue;
      if (heldPos.investedSol >= MAX_STAKE_SOL - MIN_BUY_SOL) continue;
      const pos = positions.get(mint);
      if (pos === undefined) continue;
      if (price < heldPos.lastBuyPrice * 1.05) continue;
      if (heldPos.lastValueSol < (Number(BigInt(pos.cost_lamports)) / LAMPORTS_PER_SOL) * 1.0) continue;
      if (netFlow(tape, now, FLOW_WINDOW) < MIN_NET_FLOW_SOL) continue;
      if (realSol > MAX_REAL_SOL) continue;
      wantSol = Math.min(ADD_SOL, MAX_STAKE_SOL - heldPos.investedSol);
    }
    // The 1%-of-real-reserve cap bounds every buy.
    const capSol = Math.floor((Number(curve.realQuote) * maxOrderBpsOfRealReserve) / 10_000) / LAMPORTS_PER_SOL;
    let amountSol = Math.min(capSol, wantSol, MAX_EXPOSURE_SOL - exposureSol);
    if (amountSol > freeSol - txFeeSol) amountSol = freeSol - txFeeSol;
    if (amountSol < MIN_BUY_SOL) continue;
    const lamports = BigInt(Math.floor(amountSol * LAMPORTS_PER_SOL));
    if (!push({ side: 'buy', client_id: nextClientId(), mint, quote_lamports: lamports.toString(), max_slippage_bps: BUY_SLIPPAGE_BPS })) break;
    freeSol -= amountSol + txFeeSol;
    exposureSol += amountSol;
    if (heldPos === undefined) {
      held.set(mint, { openedSlot: now, peakValueSol: 0, investedSol: amountSol, filled: false, exiting: false, lastBuyPrice: price, lastValueSol: 0 });
      tape.entries++;
      slotsLeft--;
    } else {
      heldPos.investedSol += amountSol;
      heldPos.lastBuyPrice = price;
    }
  }

  // ---- Guardrail pacing: probe round trips when fills or coins lag. ----
  if (probes.size < MAX_PROBES && slotsLeft > 0 && firstBlockTime !== null && lastBlockTime !== null) {
    const pace = Math.min(1, (lastBlockTime - firstBlockTime) / PACE_SECS);
    const needCoins = myCoins.size < Math.ceil(TARGET_COINS * pace);
    const needFills = myFills < Math.ceil(TARGET_FILLS * pace);
    if (needCoins || needFills) {
      const minRealSol = (PROBE_SOL * 10_000) / maxOrderBpsOfRealReserve;
      for (const mint of traded) {
        if (orders.length >= maxOrdersPerBatch || probes.size >= MAX_PROBES) break;
        if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || positions.has(mint)) continue;
        if (needCoins && !needFills && myCoins.has(mint)) continue;
        const curve = marks.get(mint) ?? curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (Number(curve.realQuote) / LAMPORTS_PER_SOL < minRealSol * 1.5) continue;
        if (freeSol < PROBE_SOL + 2 * txFeeSol) break;
        if (!push({ side: 'buy', client_id: nextClientId(), mint, quote_lamports: BigInt(Math.floor(PROBE_SOL * LAMPORTS_PER_SOL)).toString(), max_slippage_bps: BUY_SLIPPAGE_BPS })) break;
        probes.set(mint, false);
        freeSol -= PROBE_SOL + txFeeSol;
        slotsLeft--;
      }
    }
  }

  return orders;
}

// ---- Protocol loop: hello, batch, done over NDJSON. ----

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
let sawHello = false;

rl.on('line', (line: string) => {
  if (line.trim() === '') return;
  let msg: any;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }
  if (msg.type === 'hello') {
    sawHello = true;
    capitalLamports = BigInt(msg.capital?.lamports ?? '10000000000');
    maxOpenPositions = msg.caps?.max_open_positions ?? 5;
    maxOrderBpsOfRealReserve = msg.caps?.max_order_bps_of_real_reserve ?? 100;
    maxOrdersPerBatch = msg.caps?.max_orders_per_batch ?? 16;
    txFeeLamports =
      BigInt(msg.fees?.base_fee_lamports ?? '5000') + BigInt(msg.fees?.priority_fee_lamports ?? '100000');
    return;
  }
  if (msg.type === 'batch') {
    if (!sawHello) return;
    const orders = decideOrders(msg);
    process.stdout.write(JSON.stringify({ type: 'reply', seq: msg.seq, orders }) + '\n');
    return;
  }
  if (msg.type === 'done') {
    rl.close();
    return;
  }
});

rl.on('close', () => {
  process.exit(0);
});
