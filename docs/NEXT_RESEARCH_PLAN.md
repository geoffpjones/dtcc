# Next Research Plan

## Objective
Improve out-of-sample performance by conditioning trade selection, rather than doing more broad TP/SL tuning.

## Why This Is Next
Recent optimization attempts suggest:
- signal construction is already useful for some pairs,
- exit logic is broadly sensible,
- simple parameter searches and naive filters are not producing incremental gains.

The next likely source of improvement is better trade selection at signal time.

## Hypothesis Priority
1. Market momentum at time of signal
2. Volatility regime at time of signal
3. Moving-average confluence with limit levels

Rationale:
- momentum is most likely to distinguish high-quality reversal setups from weak ones,
- volatility changes the meaning of fixed distances, TP and SL values,
- moving averages may help, but should be treated as secondary context rather than the first filter.

## Work Plan

### 1. Build a trade-level feature dataset
For each pair and signal day, add:
- distance from market to buy level in pips
- distance from market to sell level in pips
- buy/sell band width
- nearest-strike distance
- 1D return
- 3D return
- 5D return
- 20D MA slope
- spot distance to 20D MA
- spot distance to 50D MA
- spot distance to 100D MA
- spot distance to 200D MA
- ATR(5)
- ATR(14)
- ATR(20)
- 5D realized volatility
- 20D realized volatility
- gamma concentration at selected strike
- day-over-day gamma change
- expiry concentration near selected strike

### 2. Run univariate diagnostics
Bucket each feature by pair and measure:
- fill rate
- win rate
- average pnl
- pnl per trade

Focus first on:
- momentum buckets
- volatility buckets
- ATR-adjusted distance buckets
- MA-distance buckets

### 3. Prioritize the variables that actually matter
Rank features by their ability to separate good from bad trades out of sample.

Expected priority:
1. momentum
2. volatility
3. MA confluence

### 4. Define simple candidate filters
Examples:
- only trade when distance-to-level is below an ATR threshold
- only trade reversal setups when short-term momentum is stretched
- only trade when spot is extended from the 20D or 50D MA
- only trade when gamma concentration exceeds a threshold

The goal is not to create a complex model yet. The goal is to identify simple rules that improve expectancy.

### 5. Test filters in walk-forward form
- train filter rules on the train window
- freeze them
- apply unchanged to the test window
- compare results against the current baseline by pair

This prevents over-reading in-sample relationships.

### 6. Test MA confluence as a secondary feature
Evaluate whether selected levels close to:
- 50D MA
- 100D MA
- 200D MA

improve touch probability or reversal quality.

MA confluence should be tested as an additional context variable, not as the first standalone filter.

### 7. Avoid more broad TP/SL optimization until filters are validated
Exit logic is probably not the current bottleneck.

The next gains are more likely to come from suppressing low-quality trades than from further stop/target tuning.

## Deliverables
For each pair:
- a feature table
- bucketed diagnostics
- walk-forward filter results
- performance delta versus current baseline
- recommendation on whether the pair should remain active, become conditionally active, or be suppressed

## Decision Rule
Only promote a new filter if it improves out-of-sample performance after walk-forward validation.

## Immediate Next Step
Implement the feature table for all active pairs, starting with:
- momentum
- volatility
- ATR-adjusted distance
- MA distances
