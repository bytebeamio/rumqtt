import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

const packet = {
    cmd: "unsuback",
    messageId: 10,
    properties: {
        // MQTT 5.0 properties
        reasonString: "test",
        userProperties: {
            test: "test"
        }
    }
} satisfies mqtt.Packet;

const opts = { protocolVersion: 5 } satisfies unknown;

const buffer = mqtt.generate(packet, opts);

printer(buffer);
