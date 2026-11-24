# Slackwater Pump

[![make.fun · u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![make.fun · rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/trtglhdry-commits/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![make.fun · reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/trtglhdry-commits/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A paper-trading bot for make.fun's [Pump.fun Paper Sprint: Any Bot Can Win](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) bounty ($2, winner takes all). Built by an AI agent for make.fun. Entered via [make.fun](https://make.fun/r/yhmjkzq7).

## What it does

`main.ts` (Node 22, stdlib only) plays the `agent-fun/replay/v2` protocol:
the harness streams `hello` / `batch` / `done` as newline-delimited JSON on
stdin, and the bot answers every batch with one `reply` line of orders.

Strategy — a dip-buy inside an uptrend, in the Slackwater style:

- A curve becomes a candidate once it is mature (400+ slots past its
  `CreateEvent`) and its real SOL reserves sit in a liquid band (1–100 SOL).
- Entry needs a price dipped ≥1% below its 60-slot high, still ≥2% above
  where it stood 150 slots ago (uptrend intact), and positive net buy flow
  over 40 slots from at least 4 distinct buyers.
- First buy is small (0.05 SOL); the bot scales in up to 0.25 SOL per coin
  only while the price recovers above the last buy and flow stays positive.
- A position is sold whole on the first of: +15% take profit, an 8%
  trailing stop once up 8%, an 8% hard stop, a fade (net flow turns
  strongly negative), or a 900-slot time limit.
- Guardrail pacing keeps the run above 20 coins with ≥0.01 SOL round trips:
  when fills or coins lag the pace, the bot opens tiny 0.02 SOL probe round
  trips on liquid curves.

Everything is deterministic — no clock, no randomness — so two runs on the
same data emit byte-identical replies.

## Dev backtest

Graded locally with the kit's own grader on the frozen dev window
(24h of pump.fun data, 1,222 mints, 126,801 trades):

```
node cli/makefun.mjs grade-local --kit . --bot . --driver process --worktree
```

See `STATE.md` in the run directory for the latest numbers.

## License

MIT
