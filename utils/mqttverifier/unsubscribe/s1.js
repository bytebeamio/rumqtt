import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

/** @type {mqtt.Packet} */
const packet = {
    cmd: "unsubscribe",
    messageId: 10,
    unsubscriptions: ["hello"]
};

/** @type {unknown} */
const opts = { protocolVersion: 5 };

const buffer = mqtt.generate(packet, opts);

printer(buffer);
