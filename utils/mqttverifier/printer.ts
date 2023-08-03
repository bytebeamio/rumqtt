export function printer(buffer: Buffer): void {
    let output = "[";
    for (const [i, byte] of buffer.entries()) {
        output += `0x${byte.toString(16).padStart(2, "0")}`;
        if (i < buffer.length - 1) {
            output += ", ";
        }
    }
    output += "]";

    // eslint-disable-next-line no-console
    console.log(output);
}
