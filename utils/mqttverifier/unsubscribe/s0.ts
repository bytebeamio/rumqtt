import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

const packet = {
    cmd: "unsubscribe",
    messageId: 10,
    properties: {
        // MQTT 5.0 properties
        userProperties: {
            test: "test"
        }
    },
    unsubscriptions: ["hello", "world"]
} satisfies mqtt.Packet;

const opts = { protocolVersion: 5 } satisfies unknown;

const buffer = mqtt.generate(packet, opts);

printer(buffer);
