# Introduction

Put-Call Parity describes the price relationship between puts and calls of options with the same expiry and strike price. 

Opportunities for arbitrage exist where pricing has deviated from this parity.

This script obtains option prices from the [Deribit](https://www.deribit.com/) options exchange and spot prices from various other exchanges to look for arbitrage opportunities.

# Theory

One version of the Put-Call Parity equation is as follows:  
`c + K * (1 + r)^-T = p + S_0`

where:  
`c` = price of call option  
`K` = option strike price  
`r` = risk-free interest rate  
`T` = time from now to option expiry  
`p` = price of put option  
`S_0` = prevailing spot price of the underlying asset  

and let `S_T` = price of underlying asset at expiry.  

This means that  
> Buying a call and investing an amount that matures to K at expiry  
(Left Hand Side aka LHS)    

will give us the same return as  
> Buying a put and buying the underlying asset  
 (Right Hand Side aka RHS)

**Return of each component at expiration:**     

|   | `S_T` < `K` | `S_T` > `K` |  
|:---|:---:|:---:|
|LHS: `c`| 0  | `S_T` - `K` |
|LHS: `K * (1 + r)^-T`| `K` | `K` | 
|LHS: Total | `K` | `S_T`|
|RHS: `p`| `K` - `S_T` | 0 |
|RHS: `S_0` (Long Asset)| `S_T` | `S_T`|
|RHS: Total| `K` | `S_T`|

Since the returns of both LHS and RHS are equal, if we short one and long the other, we effectively have a neutral position. 

So, at time 0, if the gain from shorting LHS is more than the cost of longing RHS, or vice versa, we have an opportunity for arbitrage.

If LHS is greater, we perform the following.  
>LHS: Sell call, borrow present value of K.  
RHS: Buy put, buy asset.  
Our net profit is `c + K * (1 + r)^-T - p - S_0`.  
This is known as a forward conversion.

If RHS is greater, we perform the following.
> LHS: Buy call, loan out/invest present value of K.  
RHS: Sell put, short asset.  
Our net profit is `p + s_0 - c - (K * (1 + r)^-T )`.
This is known as a reverse conversion.

## Simplifications
For this initial version of the script, the following simplifications will be made.
1. We will only consider the forward conversion method. The reverse conversion involves shorting the underlying asset, which involves further computation of fees and interest rates unique to each exchange. This also eats into our profits.

2. We will consider the present value of K to simply be K, ie. r = 0. Apart from the additional computation that needs to be catered to each lending platform's fee and interest structure, there is the borrowing/lending fee itself, and for a forward conversion, interest on the borrowed amount needs to be paid. These all eat into our profits.  
However, with the rise of both centralized crypto lending platforms as well as DeFi platforms, and the ability to borrow/loan crypto programmatically, this function should definitely be integrated in the future, especially for reverse conversions.


# Use Case and Strategy

This script currently looks at options available on the Deribit platform with BTC as the underlying asset. Option prices are quoted in BTC and bought or sold with BTC. The contract size is 1 BTC per contract.

We keep a certain amount of BTC on Deribit, and a certain amount of USDT on spot exchanges A and B.  

When an arbitrage opportunity presents itself, we sell and buy the call and put options respectively using BTC on Deribit, and buy BTC using USDT on the spot exchange to pre-emptively replenish the BTC supply that will be used for settlement upon expiration of the options.


The simplified version of our profit formula is now:  
`profit = c + K - p - S_0`

Substituting `c` for `c_bid * S_0`, `p` for `p_ask * S_0`, where  
`c_bid` = best call bid offer quoted in BTC  
`p_ask` = best put ask offer quoted in BTC  
`S_0` = prevailing BTC ask price found on spot exchange

we get  
  `profit = c_bid * S_0 + K - p_ask * S_0 - S_0`  
  `profit = (c_bid - p_ask - 1) * S_0 + K`
  
For profit > 0:  
`S_0 < K / (1 + p_ask - c_bid)`

**If we can find spot exchanges where BTC can be purchased for less than this `K / (1 + p_ask - c_bid)` value, we can generate profit.**

## Fees

`db_tx_fee`: transaction fee for purchase or sale of an option contract on Deribit. Quoted in BTC.  
`db_settlement_fee`: fee paid upon settlement of an option contract that expires in-the-money. Quoted in BTC. Note that in the case of a call and a put of the same expiry and strike, only one of them will expire in the money.  
`exchange_withdrawal_fee`: fee for moving assets out of exchange. Quoted in BTC.  
`exchange_tx_fee%`: transaction fee for purchase or sale of an asset, quoted as a percentage of the transaction value.

Factoring transaction fees in:  
`profit = K + (c_bid - p_ask - 1 - (2 * db_tx_fee) - db_settlement_fee - exchange_withdrawal_fee) * S_0/(1 - exchange_tx_fee%)`

For profit > 0:  
`S_0 < (1 - exchange_tx_fee%) * K / (1 + p_ask - c_bid + (2 * db_tx_fee) + db_settlement_fee + exchange_withdrawal_fee)`

## Payout

Note that when holding a short call and long put of the same expiry and strike, the payout upon expiration is always `K - S_T`. This will be positive or negative depending on whether `K` is bigger or smaller than `S_T`. Converting this to BTC, where `S_T` is the value of 1 BTC at expiration, we have   
`payout in BTC = (K / S_T) - 1 `

This means that for each short call - long put position we hold, we will gain `K / S_T` BTC and lose 1 BTC at expiration.

How ths factors into our strategy hopefully becomes clearer in the following example.

## Example

`c_bid` = 0.06 BTC  
`p_ask` = 0.0025 BTC  
`S_0` = 11,600 USDT  
`K` = 11,000 USD  
`db_tx_fee`: 0.0004 BTC  
`db_settlement_fee`: 0.0002 BTC  
`exchange_withdrawal_fee`: 0.0005 BTC  
`exchange_tx_fee%`: 0.001 (0.1%)
  
`S_0` needs to be less than 11,629.23 USDT.
Suppose we find an opportunity where `S_0` = 11,600 USDT

| Action | Deribit     | Exchange | Notes|
|:---|--------:|---:|---:|
|1. Sell Call | BTC: +0.06 -0.0004|||
|2. Buy Put | BTC: -0.0025 -0.0004|||
|3. Pre-empt Settlement Fee| BTC: -0.0002|||
|4. Pre-empt Withdrawal Fee from Exchange| | BTC: -0.0005||
|5. Buy BTC on Exchange| |BTC: +0.944, USDT: -10,961.36 |After steps 1-4, we have 0.056 BTC. We need another 0.944 BTC to make up the 1 BTC that we need to pay at expiration on Deribit.|
|6. Send BTC to Deribit| BTC: +0.935| BTC: -0.935||

Balances:  
>Deribit: 1 BTC  
Exchange: -10,961.36 USDT

Balances upon expiry:
> Deribit: 11,000/`S_T` BTC  
Exchange: -10,961.36 USDT

11,000/`S_T` BTC may be kept in BTC for future trading. At this point the profit value would be 11,000 - 10,961.36 = 38.64 USDT.


Or it may be moved to a spot exchange and converted to realize profit in USDT. Further withdrawal and transaction costs would need to be factored in here, an example computation is:  
11,000 * 0.999 - 6 - 10,972.34 = 10.66 USDT

### Other Considerations
Note that `K` is quoted in USD and for the computations here it is assumed that USD and USDT are tied at 1:1.

# Implementation

## Websocket IO
The `asyncio` module is used here. Order book information is obtained from each exchange via websocket. These populate a queue asynchronously. Items from the queue are obtained and processed by `update_worker` described below.

## Worker Processes
The `multiprocessing` module is used here. `update_worker` takes items from the queue populated with the information from the exchanges. Each of these workers can process information from any of the exchanges. Care has been taken to process these information appropriately, since each exchange has a different update format. For example, Binance sends full snapshots of their order book up to the specified depth, while Deribit sends one full snapshot at the beginning and then only sends changes subsequently. The order book information from each exchange is stored into a standardized pandas dataframes for further processing by `comp_worker`. At initialization, multiple `update_worker` processes are fired off.

`comp_worker` consolidates the information from all the order book dataframes in order to compute profitability of each option put-call pair given the available spot asks from each exchange. The profit rate per contract (1 BTC) is first calculated, then the available order sizes are taken into account to compare total profit.  
Numpy array operations are used where possible to speed up computation. 

## Database
The purpose of storing information in the database is to be able to retrieve this later and analyze the profit size of each opportunity and how long they last. 

On each computation cycle, `comp_worker` computes and filters out the put-call pairs that are profitable and prepares to do the following:
- put-call pairs that were not profitable in the previous cycle but are now - a table is created for them, if not already done so, and their information is added to the table.
- put-call pairs that were profitable in the previous cycle but no longer are - a "zero-profit' termination row is to be appended to their table.
- put-call pairs that were profitable in the previous cycle and their profit amount has changed - the new information is to be appended to their table.

A MySQL database is used.

`comp_worker` computes and prepares the statements and passes them on to the database I/O handler `sql_io`.

# To-do List and Considerations 
- Reverse conversions
- Trade execution  
    - At least 3 trades need to happen almost atomically. Need to consider management strategy if only some trades execute successfully.
    - Automate lending via DeFi or other platforms for reverse conversions. 




































