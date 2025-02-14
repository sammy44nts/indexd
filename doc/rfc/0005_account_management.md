# Account Management

## Abstract

This RFC describes the user account management for the `indexer`. Users are
expected to create one account per app they want to use with the indexer. An account is a ed25519 keypair and
serves the following purposes:

1. Authentication: Apps sign requests with the corresponding private key of the account as an alternative to Basic Auth
2. Slab Isolation: Slabs are associated with an account and can only be removed by the account owner to prevent one app accessing data from another app
3. Payment: The public key of the account is funded by the `indexer` on all hosts the `indexer` has good contracts with

### Authentication

TODO: Probably don't need special authentication for now. Probably fine to add
that right before we allow users to access third party indexers

### Account Funding

Unlike `renterd`, the `indexer` doesn't locally track the expected balance of
accounts to know when to fund them. Instead, it uses a ratelimit-based approach.
Accounts are funded with at most 1SC per hour. To do so, the following state
will be stored for every account and host:

| **Field** | **Description** |
|-------|-------------|
| Account | The public key of the account |
| TimeToFund | The time when the account will be funded again |
| Host | The host the account is funded on |

The `indexer` will then perform the following steps for each host it has a contract with:
1. Fetch all accounts for that host where `TimeToFund` is in the past
2. In batches of up to 100, fund the accounts with 1SC each. If the remaining funds in the contract are too low, fund fewer accounts.
3. For each account in the batch:
  3a. Upon success, update `TimeToFund` to the `MIN(6 hours , current time + MAX(new account balance, 1SC) / 1SC * 1 hour (rounded up))` to avoid funding accounts at a higher rate than 1SC/hour
  3b. Upon failure, log the error and update `TimeToFund` using an exponential backoff (1, 2, 4, 8, 16, 32, 64 and 128 minutes)
4. Sleep until the lowest `TimeToFund` for an account on this host is reached or until the next account is registered

NOTE: `MAX(new account balance, 1SC)` is used to prevent hosts that return `0SC`
as the new balance from sending us into a hot refill-loop.

**Costs**

While the rate-based approach has many advantages over the approach used in
`renterd`, it also has one drawback. The `indexer` never fetches the balance of
an account before trying to fund it. So we will continuously pay hosts. So let's
do some math to figure out the actual cost involved with maintaining an account.
There are 3 extreme cases to consider:

1. Ideal case: The account is drained at the funding rate
2. Worst case on good host: The account is not used and the `indexer` keeps funding it
3. Worst case on bad host: The account is not used and the host keeps claiming the balance is 0SC

For case 1, the loss is 0SC since it all gets used up but for case 2 it's not as straightforward.
The following table is a breakdown of funding an account on a good host that doesn't get used:

| **New Balance** | **Sleep duration** | **New Time (hours since account creation)** |
|-----------------|----------|-----------------|
| **First Day** |
| 1 SC | 1 hour  | +1h |
| 2 SC | 2 hours | +3h |
| 3 SC | 3 hours | +6h |
| 4 SC | 4 hours | +10h |
| 5 SC | 5 hours | +15h |
| 6 SC | 6 hours | +21h |
| **Second Day** |
| 7 SC | 6 hours | +27h |
| 8 SC | 6 hours | +33h |
| 9 SC | 6 hours | +39h |
| 10 SC | 6 hours | +45h |

After ~1 day (21 hours), the account balance reaches 6SC. That's when the max
time between funding of 6 hours is reached. On the second day, the account
reaches a balance of 10SC. From then on, the account will grow by 4SC a day.
That means the total monthly cost of maintaining that account is 120SC.

For case 3, the numbers look quite different. Note that the new balance is
always 1SC since the host claims that the balance was 0SC before funding:

| **New Balance** | **Sleep duration** | **New Time (hours since account creation)** |
|-----------------|----------|-----------------|
| 1 SC | 1 hour  | +1h |
| 1 SC | 1 hour | +2h |
| 1 SC | 1 hour | +3h |
| ... | ... | ... |
| 1 SC | 1 hour | +24h |

The account will effectively be funded with 1SC every hour, leading to a total
loss of 24SC a day or 720SC a month per account.

### Discussion

At the time of writing the costs above are about $0.5 a month for an honest host
and $3 for a cheating host per account if the user doesn't upload/download at
all. Assuming a third party wants to provide a $10 subscription service, that
service can't be profitable as soon as they maintain 20 accounts with good hosts
for a user or >3 accounts with malicious hosts. Meaning at 100 accounts with
good hosts the service loses $50 a month per app per user without the user
actively using the service.

While we can probably use metrics and heuristics to identify bad hosts and block
them, we should probably refine the algorithm in regards to idle good hosts.
e.g. once the cap is reached we could switch to fetching the balance of an
account before funding it.
