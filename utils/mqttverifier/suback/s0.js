import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

/** @type {mqtt.Packet} */
const packet = {
    cmd: "suback",
    messageId: 42,
    properties: {
        // MQTT 5.0 properties
        reasonString: "test",
        userProperties: {
            test: "test"
        }
    },
    granted: [0, 1, 2, 128]
};

/** @type {unknown} */
const opts = { protocolVersion: 5 };

const buffer = mqtt.generate(packet, opts);

printer(buffer);
