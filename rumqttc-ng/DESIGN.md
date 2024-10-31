
- All components are loosely coupled and communicate via xchg
  - Builder takes care stitching everything together by picking relavant compnents based on builder configuration
  - E.g eventloop doesn't know anything about transport or clients
  - This helps enhance the codebase to support more transports and platforms like web or microcontrollers
