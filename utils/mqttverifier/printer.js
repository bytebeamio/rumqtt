/**
 * Print the buffer to console.
 * 
 * @param {Buffer} buffer - Buffer
 * @returns {void}
 */
export function printer(buffer) {
    let output = "[";
    for (const [i, byte] of buffer.entries()) {
        output += `0x${byte.toString(16).padStart(2, "0")}`;
        if (i < buffer.length - 1) {
            output += ", ";
        }
    }
    output += "]";

    console.log(output);
}
