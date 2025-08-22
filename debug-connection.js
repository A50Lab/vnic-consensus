const { Tendermint34Client } = require("@cosmjs/tendermint-rpc");

async function debugConnection() {
    try {
        console.log("Connecting to Tendermint RPC...");
        const tmClient = await Tendermint34Client.connect("http://127.0.0.1:26656");

        console.log("Getting status...");
        const status = await tmClient.status();
        console.log("Status received:", JSON.stringify(status, null, 2));

        console.log("Getting genesis...");
        const genesis = await tmClient.genesis();
        console.log("Genesis validators:", genesis.genesis.validators.slice(0, 1));

        tmClient.disconnect();
    } catch (error) {
        console.error("Error:", error.message);
        console.error("Stack:", error.stack);
    }
}

debugConnection();
