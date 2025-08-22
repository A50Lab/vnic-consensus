const { StargateClient, SigningStargateClient } = require("@cosmjs/stargate");
const { toBech32, fromHex, toHex } = require("@cosmjs/encoding");
const { Sha256, Random } = require("@cosmjs/crypto");
const { fromBase64, toBase64 } = require("@cosmjs/encoding");
const { DirectSecp256k1HdWallet } = require("@cosmjs/proto-signing");
const { coins } = require("@cosmjs/stargate");
const { CometClient } = require("@cosmjs/tendermint-rpc");

async function checkBalance(mnemonic, rpcEndpoint, addressPrefix = "cosmos") {
    try {
        const wallet = await DirectSecp256k1HdWallet.fromMnemonic(mnemonic, {
            prefix: addressPrefix,
        });
        const [firstAccount] = await wallet.getAccounts();
        const address = firstAccount.address;

        console.log(`Wallet address: ${address}`);

        console.log("Connecting to RPC endpoint...");
        const client = await StargateClient.connect(rpcEndpoint);

        console.log("Querying balances...");
        const balance = await client.getAllBalances(address);

        console.log("Balances:");
        if (balance.length === 0) {
            console.log("  No tokens found");
        } else {
            balance.forEach((coin) => {
                console.log(`  ${coin.amount} ${coin.denom}`);
            });
        }

        client.disconnect();
    } catch (error) {
        console.error("Error checking balance:", error.message);
    }
}

function generateRandomAddress(addressPrefix = "cosmos") {
    const randomBytes = Random.getBytes(20);
    const address = toBech32(addressPrefix, randomBytes);
    return address;
}

async function sendMoney(
    mnemonic,
    recipientAddress,
    amount,
    denom,
    rpcEndpoint,
    addressPrefix = "cosmos"
) {
    try {
        const wallet = await DirectSecp256k1HdWallet.fromMnemonic(mnemonic, {
            prefix: addressPrefix,
        });
        const [firstAccount] = await wallet.getAccounts();
        const senderAddress = firstAccount.address;

        console.log(`Sender address: ${senderAddress}`);
        console.log(`Recipient address: ${recipientAddress}`);

        const client = await SigningStargateClient.connectWithSigner(rpcEndpoint, wallet);

        const fee = {
            amount: coins(1, denom),
            gas: "200000",
        };

        const result = await client.sendTokens(
            senderAddress,
            recipientAddress,
            coins(amount, denom),
            fee,
            "Sending money to random address"
        );

        console.log(`Transaction successful: ${result.transactionHash}`);
        console.log(`Height: ${result.height}`);

        client.disconnect();
        return result;
    } catch (error) {
        console.error("Error sending money:", error.message);
        throw error;
    }
}

// Use the funded test wallet that we successfully funded earlier
const MNEMONIC =
    "upon stem myth silver whale gadget giraffe exist cannon clip spike pilot";
const RPC_ENDPOINT = "http://127.0.0.1:26657";
const ADDRESS_PREFIX = "vnic";
const DENOM = "stake";

async function main() {
    console.log("=== Checking initial balance ===");
    await checkBalance(MNEMONIC, RPC_ENDPOINT, ADDRESS_PREFIX);

    console.log("\n=== Generating random address ===");
    const randomAddress = generateRandomAddress(ADDRESS_PREFIX);
    console.log(`Generated random address: ${randomAddress}`);

    console.log("\n=== Sending money to random address ===");
    try {
        await sendMoney(MNEMONIC, randomAddress, "2", DENOM, RPC_ENDPOINT, ADDRESS_PREFIX);
        console.log("Money sent successfully!");

        console.log("\n=== Checking balance of random address ===");
        const { StargateClient } = require("@cosmjs/stargate");
        const client = await StargateClient.connect(RPC_ENDPOINT);
        const balance = await client.getAllBalances(randomAddress);
        console.log(`Random address balance:`);
        if (balance.length === 0) {
            console.log("  No tokens found");
        } else {
            balance.forEach((coin) => {
                console.log(`  ${coin.amount} ${coin.denom}`);
            });
        }
        client.disconnect();
    } catch (error) {
        console.error("Failed to send money:", error.message);
    }
}

main().catch(console.error);
