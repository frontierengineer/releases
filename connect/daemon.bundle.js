#!/usr/bin/env node

if (typeof Symbol.dispose !== 'symbol') {
  Object.defineProperty(Symbol, 'dispose', { value: Symbol('Symbol.dispose') });
}
if (typeof Symbol.asyncDispose !== 'symbol') {
  Object.defineProperty(Symbol, 'asyncDispose', { value: Symbol('Symbol.asyncDispose') });
}
if (typeof globalThis.structuredClone !== 'function') {
  globalThis.structuredClone = function structuredCloneShim(v) {
    return v == null ? v : JSON.parse(JSON.stringify(v));
  };
}
if (typeof globalThis.fetch !== 'function') {
  globalThis.fetch = function fetchUnavailableOnThisNode() {
    throw new Error('global fetch() is not available on this node version; the frontier worker daemon does not use it — this call indicates an unexpected SDK code path');
  };
}

"use strict";
var __getOwnPropNames = Object.getOwnPropertyNames;
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports2, module2) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob) BINARY_TYPES.push("blob");
    module2.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: /* @__PURE__ */ Symbol("kIsForOnEventAttribute"),
      kListener: /* @__PURE__ */ Symbol("kListener"),
      kStatusCode: /* @__PURE__ */ Symbol("status-code"),
      kWebSocket: /* @__PURE__ */ Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports2, module2) {
    "use strict";
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module2.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = require("bufferutil");
        module2.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bufferUtil.mask(source, mask, output, offset, length);
        };
        module2.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports2, module2) {
    "use strict";
    var kDone = /* @__PURE__ */ Symbol("kDone");
    var kRun = /* @__PURE__ */ Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency) return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module2.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports2, module2) {
    "use strict";
    var zlib = require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = /* @__PURE__ */ Symbol("permessage-deflate");
    var kTotalLength = /* @__PURE__ */ Symbol("total-length");
    var kCallback = /* @__PURE__ */ Symbol("callback");
    var kBuffers = /* @__PURE__ */ Symbol("buffers");
    var kError = /* @__PURE__ */ Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && !params.client_max_window_bits) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num = +value;
                if (!Number.isInteger(num) || num < 8 || num > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module2.exports = PerMessageDeflate;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports2, module2) {
    "use strict";
    var { isUtf8 } = require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module2.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module2.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = require("utf-8-validate");
        module2.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports2, module2) {
    "use strict";
    var { Writable } = require("stream");
    var PerMessageDeflate = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored) cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num = buf.readUInt32BE(0);
        if (num > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO) this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module2.exports = Receiver;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports2, module2) {
    "use strict";
    var { Duplex } = require("stream");
    var { randomFillSync } = require("crypto");
    var PerMessageDeflate = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = /* @__PURE__ */ Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask) return [target, data];
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (skipMasking) return [target, data];
        if (merge) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else {
            buf.set(data, 2);
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module2.exports = Sender;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function") cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function") callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports2, module2) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = /* @__PURE__ */ Symbol("kCode");
    var kData = /* @__PURE__ */ Symbol("kData");
    var kError = /* @__PURE__ */ Symbol("kError");
    var kMessage = /* @__PURE__ */ Symbol("kMessage");
    var kReason = /* @__PURE__ */ Symbol("kReason");
    var kTarget = /* @__PURE__ */ Symbol("kTarget");
    var kType = /* @__PURE__ */ Symbol("kType");
    var kWasClean = /* @__PURE__ */ Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module2.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0) dest[name] = [elem];
      else dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            let value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1) end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension) => {
        let configurations = extensions[extension];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module2.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var https = require("https");
    var http = require("http");
    var net = require("net");
    var tls = require("tls");
    var { randomBytes, createHash } = require("crypto");
    var { Duplex, Readable } = require("stream");
    var { URL } = require("url");
    var PerMessageDeflate = require_permessage_deflate();
    var Receiver = require_receiver();
    var Sender = require_sender();
    var { isBlob } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = /* @__PURE__ */ Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            protocols = [];
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              protocols = [];
            } else {
              protocols = [protocols];
            }
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._autoPong = options.autoPong;
          this._closeTimeout = options.closeTimeout;
          this._isServer = true;
        }
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout) socket.setTimeout(0);
        if (socket.setNoDelay) socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate.extensionName]) {
          this._extensions[PerMessageDeflate.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._readyState = _WebSocket.CLOSING;
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain) this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function") return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module2.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      websocket._autoPong = opts.autoPong;
      websocket._closeTimeout = opts.closeTimeout;
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:") {
        parsedUrl.protocol = "ws:";
      } else if (parsedUrl.protocol === "https:") {
        parsedUrl.protocol = "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes(16).toString("base64");
      const request = isSecure ? https.request : http.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
      }
      if (isIpcUrl) {
        const parts = opts.path.split(":");
        opts.socketPath = parts[0];
        opts.path = parts[1];
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost) delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted]) return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream[kAborted] = true;
        stream.abort();
        if (stream.socket && !stream.socket.destroyed) {
          stream.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob(data) ? data.size : toBuffer(data).length;
        if (websocket._socket) websocket._sender._bufferedBytes += length;
        else websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (websocket._socket[kWebSocket] === void 0) return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused) websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED) return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports2, module2) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream(ws2, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws2.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data)) ws2.pause();
      });
      ws2.once("error", function error(err) {
        if (duplex.destroyed) return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws2.once("close", function close() {
        if (duplex.destroyed) return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws2.readyState === ws2.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws2.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws2.once("close", function close() {
          if (!called) callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy) ws2.terminate();
      };
      duplex._final = function(callback) {
        if (ws2.readyState === ws2.CONNECTING) {
          ws2.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws2._socket === null) return;
        if (ws2._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted) duplex.destroy();
        } else {
          ws2._socket.once("finish", function finish() {
            callback();
          });
          ws2.close();
        }
      };
      duplex._read = function() {
        if (ws2.isPaused) ws2.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws2.readyState === ws2.CONNECTING) {
          ws2.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws2.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module2.exports = createWebSocketStream;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1) end = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1) end = i;
          const protocol2 = header.slice(start, end);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module2.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var http = require("http");
    var { Duplex } = require("stream");
    var { createHash } = require("crypto");
    var extension = require_extension();
    var PerMessageDeflate = require_permessage_deflate();
    var subprotocol = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxPayload: 100 * 1024 * 1024,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http.createServer((req, res) => {
            const body = http.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb) this.once("close", cb);
        if (this._state === CLOSING) return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate.extensionName]);
              extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws2 = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws2._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate.extensionName]) {
          const params = extensions[PerMessageDeflate.extensionName].params;
          const value = extension.format({
            [PerMessageDeflate.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws2._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws2.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws2);
          ws2.on("close", () => {
            this.clients.delete(ws2);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws2, req);
      }
    };
    module2.exports = WebSocketServer;
    function addListeners(server, map) {
      for (const event of Object.keys(map)) server.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server) {
      server._state = CLOSED;
      server.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server, req, socket, code, message, headers) {
      if (server.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// node_modules/ws/index.js
var require_ws = __commonJS({
  "node_modules/ws/index.js"(exports2, module2) {
    "use strict";
    var createWebSocketStream = require_stream();
    var extension = require_extension();
    var PerMessageDeflate = require_permessage_deflate();
    var Receiver = require_receiver();
    var Sender = require_sender();
    var subprotocol = require_subprotocol();
    var WebSocket2 = require_websocket();
    var WebSocketServer = require_websocket_server();
    WebSocket2.createWebSocketStream = createWebSocketStream;
    WebSocket2.extension = extension;
    WebSocket2.PerMessageDeflate = PerMessageDeflate;
    WebSocket2.Receiver = Receiver;
    WebSocket2.Sender = Sender;
    WebSocket2.Server = WebSocketServer;
    WebSocket2.subprotocol = subprotocol;
    WebSocket2.WebSocket = WebSocket2;
    WebSocket2.WebSocketServer = WebSocketServer;
    module2.exports = WebSocket2;
  }
});

// machine/workerCrypto.js
var require_workerCrypto = __commonJS({
  "machine/workerCrypto.js"(exports2, module2) {
    "use strict";
    var crypto2 = require("crypto");
    var X25519_SPKI_PREFIX = Buffer.from("302a300506032b656e032100", "hex");
    var NONCE_BYTES = 12;
    var TAG_BYTES = 16;
    var KEY_BYTES = 32;
    var HKDF_SALT = Buffer.from("frontier-worker-link-v1", "utf8");
    var INFO_HOST_TO_WORKER = Buffer.from("h2w", "utf8");
    var INFO_WORKER_TO_HOST = Buffer.from("w2h", "utf8");
    function generateEphemeralKeyPair() {
      return crypto2.generateKeyPairSync("x25519");
    }
    function exportPublicKeyRaw(publicKey) {
      return publicKey.export({ type: "spki", format: "der" }).subarray(X25519_SPKI_PREFIX.length);
    }
    function importPublicKeyRaw(raw) {
      const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
      if (buf.length !== KEY_BYTES) throw new Error(`x25519 public key must be ${KEY_BYTES} bytes, got ${buf.length}`);
      return crypto2.createPublicKey({ key: Buffer.concat([X25519_SPKI_PREFIX, buf]), format: "der", type: "spki" });
    }
    function hkdf(secret, info) {
      return Buffer.from(crypto2.hkdfSync("sha256", secret, HKDF_SALT, info, KEY_BYTES));
    }
    function deriveSessionKeys(sharedSecret, authenticator) {
      const auth = Buffer.isBuffer(authenticator) ? authenticator : Buffer.from(String(authenticator || ""), "utf8");
      const ikm = Buffer.concat([Buffer.from([sharedSecret.length]), sharedSecret, auth]);
      return {
        hostToWorker: hkdf(ikm, INFO_HOST_TO_WORKER),
        workerToHost: hkdf(ikm, INFO_WORKER_TO_HOST)
      };
    }
    var DirectionCipher = class {
      constructor(key) {
        this.key = key;
        this.counter = 0n;
      }
      nextNonce() {
        const nonce = Buffer.alloc(NONCE_BYTES);
        nonce.writeBigUInt64BE(this.counter, NONCE_BYTES - 8);
        this.counter += 1n;
        return nonce;
      }
      // plaintext: Buffer -> framed Buffer [nonce][ct][tag]. AAD optional.
      seal(plaintext, aad) {
        const nonce = this.nextNonce();
        const cipher = crypto2.createCipheriv("chacha20-poly1305", this.key, nonce, { authTagLength: TAG_BYTES });
        if (aad) cipher.setAAD(aad);
        const ct = Buffer.concat([cipher.update(plaintext), cipher.final()]);
        return Buffer.concat([nonce, ct, cipher.getAuthTag()]);
      }
      // framed Buffer -> plaintext Buffer. Throws on a bad tag (caller drops the
      // connection — a bad tag means tampering or a desynced key).
      open(frame, aad) {
        if (frame.length < NONCE_BYTES + TAG_BYTES) throw new Error("frame too short");
        const nonce = frame.subarray(0, NONCE_BYTES);
        const tag = frame.subarray(frame.length - TAG_BYTES);
        const ct = frame.subarray(NONCE_BYTES, frame.length - TAG_BYTES);
        const decipher = crypto2.createDecipheriv("chacha20-poly1305", this.key, nonce, { authTagLength: TAG_BYTES });
        if (aad) decipher.setAAD(aad);
        decipher.setAuthTag(tag);
        return Buffer.concat([decipher.update(ct), decipher.final()]);
      }
    };
    function createSession(sharedSecret, authenticator, role) {
      const keys = deriveSessionKeys(sharedSecret, authenticator);
      const sealKey = role === "host" ? keys.hostToWorker : keys.workerToHost;
      const openKey = role === "host" ? keys.workerToHost : keys.hostToWorker;
      const sealer = new DirectionCipher(sealKey);
      const opener = new DirectionCipher(openKey);
      return {
        role,
        sealText(obj) {
          return sealer.seal(Buffer.from(JSON.stringify(obj), "utf8"));
        },
        openText(frame) {
          return JSON.parse(opener.open(frame).toString("utf8"));
        },
        seal(buf) {
          return sealer.seal(buf);
        },
        open(frame) {
          return opener.open(frame);
        }
      };
    }
    function computeSharedSecret(privateKey, peerPublicKeyRaw) {
      return crypto2.diffieHellman({ privateKey, publicKey: importPublicKeyRaw(peerPublicKeyRaw) });
    }
    function generateStaticKeyPair() {
      return crypto2.generateKeyPairSync("x25519");
    }
    function exportPrivateKeyRaw(privateKey) {
      const der = privateKey.export({ type: "pkcs8", format: "der" });
      return der.subarray(der.length - KEY_BYTES);
    }
    var X25519_PKCS8_PREFIX = Buffer.from("302e020100300506032b656e04220420", "hex");
    function importPrivateKeyRaw(raw) {
      const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
      if (buf.length !== KEY_BYTES) throw new Error(`x25519 private key must be ${KEY_BYTES} bytes, got ${buf.length}`);
      return crypto2.createPrivateKey({ key: Buffer.concat([X25519_PKCS8_PREFIX, buf]), format: "der", type: "pkcs8" });
    }
    function concatSecrets(...secrets) {
      return Buffer.concat(secrets);
    }
    module2.exports = {
      X25519_SPKI_PREFIX,
      NONCE_BYTES,
      TAG_BYTES,
      KEY_BYTES,
      generateEphemeralKeyPair,
      generateStaticKeyPair,
      exportPublicKeyRaw,
      importPublicKeyRaw,
      exportPrivateKeyRaw,
      importPrivateKeyRaw,
      deriveSessionKeys,
      computeSharedSecret,
      concatSecrets,
      createSession,
      DirectionCipher
    };
  }
});

// machine/worker/extensionLoader.js
var require_extensionLoader = __commonJS({
  "machine/worker/extensionLoader.js"(exports2, module2) {
    "use strict";
    var fs2 = require("fs");
    var os2 = require("os");
    var path2 = require("path");
    var http = require("http");
    var runtimes = /* @__PURE__ */ new Map();
    var components = /* @__PURE__ */ new Map();
    function importWorker(specifier) {
      return import(specifier);
    }
    function requireWorker(specifier) {
      return require(specifier);
    }
    function makeRuntimeProvider(extensionId, onRegister, config) {
      const v1 = {
        id: extensionId,
        register(impl) {
          onRegister(impl);
        },
        services: {
          config: config || { declare() {
          }, get() {
            return void 0;
          }, watch() {
            return () => {
            };
          } },
          importWorker
        },
        deregister() {
        }
      };
      return {
        version(v) {
          if (v === 1) return v1;
          throw new Error(`[${extensionId}] runtime capability v${v} is not supported (supported: 1)`);
        }
      };
    }
    function makeWorkerProvider(extensionId, record, deps) {
      const v1 = {
        id: extensionId,
        channel: {
          send(payload) {
            const ok = deps.send({ type: "ext.msg", extension: extensionId, machine: deps.machine, payload });
            if (!ok) console.error(`[extension-loader] ${extensionId}: ext.msg dropped (link down)`);
          },
          onMessage(handler) {
            record.handlers.add(handler);
            return () => {
              record.handlers.delete(handler);
            };
          }
        },
        services: {
          importWorker,
          requireWorker,
          hostUrl: deps.baseUrl,
          // Fetch a worker-plane asset (e.g. a node-pty prebuilt tarball) the
          // transport-correct way: over the control WS on a paired/relayed
          // connection (returns a Buffer), or null when the component should use
          // plain HTTP against hostUrl (direct/legacy). `workerPath` is under
          // /worker, e.g. `/daemon-deps/node-pty/<key>`.
          fetchAsset: deps.fetchAsset || null
        },
        deregister() {
        }
      };
      return {
        version(v) {
          if (v === 1) return v1;
          throw new Error(`[${extensionId}] worker capability v${v} is not supported (supported: 1)`);
        }
      };
    }
    function fetchText(url) {
      return new Promise((resolve, reject) => {
        http.get(url, (res) => {
          if (res.statusCode !== 200) {
            res.resume();
            reject(new Error(`GET ${url} -> ${res.statusCode}`));
            return;
          }
          let data = "";
          res.setEncoding("utf8");
          res.on("data", (c) => {
            data += c;
          });
          res.on("end", () => resolve(data));
        }).on("error", reject);
      });
    }
    async function fetchAssetText({ baseUrl, fetchAsset, workerPath }) {
      if (fetchAsset) {
        const buf = await fetchAsset(workerPath);
        return Buffer.isBuffer(buf) ? buf.toString("utf8") : String(buf);
      }
      return fetchText(`${baseUrl}${workerPath}`);
    }
    function requireFromText(text, label) {
      const dir = fs2.mkdtempSync(path2.join(os2.tmpdir(), `frontier-${label}-`));
      const file = path2.join(dir, "bundle.js");
      fs2.writeFileSync(file, text);
      return require(file);
    }
    async function loadRuntime({ baseUrl, extensionId, hash, config, fetchAsset }) {
      const workerPath = `/extensions/${extensionId}/runtime.bundle.js${hash ? `?h=${hash}` : ""}`;
      const text = await fetchAssetText({ baseUrl, fetchAsset, workerPath });
      const mod = requireFromText(text, `runtime-${extensionId}`);
      if (typeof mod.register !== "function") {
        throw new Error(`[${extensionId}] runtime bundle does not export register()`);
      }
      let impl = null;
      mod.register(makeRuntimeProvider(extensionId, (i) => {
        impl = i;
      }, config));
      if (!impl || typeof impl.run !== "function") {
        throw new Error(`[${extensionId}] runtime registered no run()`);
      }
      runtimes.set(extensionId, impl);
      console.log(`[extension-loader] loaded runtime: ${extensionId}`);
      return impl;
    }
    async function loadWorkerComponent({ baseUrl, extensionId, hash, machine, send: send2, fetchAsset }) {
      const workerPath = `/extensions/${extensionId}/worker.bundle.js${hash ? `?h=${hash}` : ""}`;
      const text = await fetchAssetText({ baseUrl, fetchAsset, workerPath });
      const mod = requireFromText(text, `worker-${extensionId}`);
      if (typeof mod.register !== "function") {
        throw new Error(`[${extensionId}] worker bundle does not export register()`);
      }
      const record = { handlers: /* @__PURE__ */ new Set() };
      mod.register(makeWorkerProvider(extensionId, record, { baseUrl, machine, send: send2, fetchAsset }));
      components.set(extensionId, record);
      console.log(`[extension-loader] loaded worker component: ${extensionId}`);
    }
    async function loadFromHost({ baseUrl, machine, send: send2, config, fetchAsset }) {
      let entries = [];
      try {
        const txt = await fetchAssetText({ baseUrl, fetchAsset, workerPath: "/extensions/manifest" });
        const parsed = JSON.parse(txt);
        entries = Array.isArray(parsed) ? parsed : parsed && parsed.extensions || [];
      } catch (err) {
        console.error("[extension-loader] manifest fetch failed:", err && err.message || err);
        return runtimes;
      }
      for (const e of entries) {
        if (!e || !e.id) continue;
        if (e.hasRuntime && !runtimes.has(e.id)) {
          try {
            await loadRuntime({ baseUrl, extensionId: e.id, hash: e.runtimeHash, config, fetchAsset });
          } catch (err) {
            console.error(`[extension-loader] failed to load runtime ${e.id}:`, err && err.message || err);
          }
        }
        if (e.hasWorker && !components.has(e.id)) {
          try {
            await loadWorkerComponent({ baseUrl, extensionId: e.id, hash: e.workerHash, machine, send: send2, fetchAsset });
          } catch (err) {
            console.error(`[extension-loader] failed to load worker component ${e.id}:`, err && err.message || err);
          }
        }
      }
      return runtimes;
    }
    function dispatchWorkerMessage(extensionId, payload) {
      const record = components.get(extensionId);
      if (!record || record.handlers.size === 0) {
        console.log(`[extension-loader] ext.msg for unloaded component "${extensionId}" dropped`);
        return;
      }
      for (const handler of Array.from(record.handlers)) {
        try {
          handler(payload);
        } catch (err) {
          console.error(`[extension-loader] ${extensionId} component handler error:`, err && err.message || err);
        }
      }
    }
    function runtimeFor(extensionId) {
      return runtimes.get(extensionId) || null;
    }
    function allRuntimes() {
      return Array.from(runtimes.values());
    }
    module2.exports = { loadFromHost, dispatchWorkerMessage, runtimeFor, allRuntimes, runtimes, components };
  }
});

// machine/daemon.js
var WebSocket = require_ws();
var path = require("path");
var fs = require("fs");
var fsp = require("fs/promises");
var os = require("os");
var crypto = require("crypto");
var workerCrypto = require_workerCrypto();
var SOURCE_HASH = (() => {
  try {
    return crypto.createHash("sha256").update(fs.readFileSync(__filename)).digest("hex");
  } catch {
    return null;
  }
})();
var MACHINE_UUID = process.env.FRONTIER_MACHINE_UUID || null;
var MACHINE_TOKEN = process.env.FRONTIER_WORKER_TOKEN || null;
var PAIRED = false;
var HOST_STATIC_PUB = null;
var DAEMON_REACH_ID = null;
var cryptoSession = null;
var DEFAULT_LINK_URL = "https://frontier-link-1036424375796.us-central1.run.app";
var FRONTIER_LINK_URL = process.env.FRONTIER_LINK_URL || DEFAULT_LINK_URL;
var SERVER_HOST = process.env.FRONTIER_SERVER_HOST || "localhost:61815";
var EXPLICIT_HOST = null;
var PID_FILE = null;
function claimPidLock() {
  PID_FILE = path.join(os.tmpdir(), "frontier-worker", `daemon.${MACHINE_UUID}.pid`);
  (function killPriorDaemon() {
    try {
      const raw = fs.readFileSync(PID_FILE, "utf-8").trim();
      const oldPid = parseInt(raw, 10);
      if (!oldPid || oldPid === process.pid) return;
      try {
        process.kill(oldPid, 0);
      } catch {
        return;
      }
      console.log(`[daemon] prior daemon for ${MACHINE_UUID} alive at PID=${oldPid}; killing`);
      try {
        process.kill(oldPid, "SIGTERM");
      } catch {
      }
      const deadline = Date.now() + 5e3;
      while (Date.now() < deadline) {
        try {
          process.kill(oldPid, 0);
        } catch {
          return;
        }
        const end = Date.now() + 100;
        while (Date.now() < end) {
        }
      }
      try {
        process.kill(oldPid, "SIGKILL");
      } catch {
      }
    } catch {
    }
  })();
  try {
    fs.mkdirSync(path.dirname(PID_FILE), { recursive: true });
    fs.writeFileSync(PID_FILE, String(process.pid));
  } catch (err) {
    console.error(`[daemon] could not write PID file ${PID_FILE}: ${err && err.message || err}`);
  }
}
function cleanupPidFile() {
  if (!PID_FILE) return;
  try {
    const raw = fs.readFileSync(PID_FILE, "utf-8").trim();
    if (parseInt(raw, 10) === process.pid) fs.unlinkSync(PID_FILE);
  } catch {
  }
}
process.on("exit", cleanupPidFile);
process.on("SIGTERM", () => {
  cleanupPidFile();
  process.exit(0);
});
process.on("SIGINT", () => {
  cleanupPidFile();
  process.exit(0);
});
var extensionLoader = require_extensionLoader();
var runtimesLoaded = false;
var ws = null;
var reconnectDelay = 1e3;
var DAEMON_START_TS = (/* @__PURE__ */ new Date()).toISOString();
var inflight = /* @__PURE__ */ new Map();
function trackInflight(executionId, ac, meta) {
  inflight.set(executionId, { ac, meta: meta || {} });
}
function inflightMetaFromMsg(msg) {
  return {
    role: msg.role || null,
    reservationId: msg.reservationId || null,
    sessionId: msg.sessionId || null,
    dispatchedAt: (/* @__PURE__ */ new Date()).toISOString()
  };
}
function send(obj) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  if (cryptoSession) ws.send(cryptoSession.sealText(obj));
  else ws.send(JSON.stringify(obj));
  return true;
}
function frameToBuf(data) {
  if (Buffer.isBuffer(data)) return data;
  if (Array.isArray(data)) return Buffer.concat(data);
  return Buffer.from(data);
}
function decodeFrame(data) {
  if (cryptoSession) return cryptoSession.openText(frameToBuf(data));
  return JSON.parse(data.toString());
}
async function sweepDescendantProcesses() {
  if (process.platform !== "linux") return;
  const myPid = process.pid;
  let pids;
  try {
    pids = await fsp.readdir("/proc");
  } catch {
    return;
  }
  const parents = /* @__PURE__ */ new Map();
  await Promise.all(pids.map(async (entry) => {
    const pid = parseInt(entry, 10);
    if (!pid || pid === myPid) return;
    try {
      const status = await fsp.readFile(`/proc/${entry}/status`, "utf-8");
      const m = /^PPid:\s+(\d+)/m.exec(status);
      if (m) parents.set(pid, parseInt(m[1], 10));
    } catch {
    }
  }));
  const isDescendant = (pid) => {
    let cur = parents.get(pid);
    while (cur && cur !== 1) {
      if (cur === myPid) return true;
      cur = parents.get(cur);
    }
    return false;
  };
  const targets = [];
  for (const pid of parents.keys()) if (isDescendant(pid)) targets.push(pid);
  if (!targets.length) return;
  console.log(`[daemon] sweeping ${targets.length} descendant process(es): ${targets.join(", ")}`);
  for (const pid of targets) {
    try {
      process.kill(pid, "SIGTERM");
    } catch {
    }
  }
  setTimeout(() => {
    for (const pid of targets) {
      try {
        process.kill(pid, 0);
      } catch {
        continue;
      }
      try {
        process.kill(pid, "SIGKILL");
      } catch {
      }
    }
  }, 3e3);
}
function onConnectionReady() {
  console.log(`[daemon] connected as ${MACHINE_UUID}${cryptoSession ? " (encrypted)" : ""}`);
  reconnectDelay = 1e3;
  sendHeartbeat();
  if (!runtimesLoaded) {
    const fetchAsset = cryptoSession ? wsFetchAsset : null;
    extensionLoader.loadFromHost({ baseUrl: `http://${SERVER_HOST}/worker`, machine: MACHINE_UUID, send, fetchAsset }).then(() => {
      runtimesLoaded = extensionLoader.runtimes.size > 0;
      console.log(`[daemon] runtimes loaded: ${extensionLoader.allRuntimes().length}, worker components: ${extensionLoader.components.size}`);
    }).catch((err) => console.error(`[daemon] extension load failed: ${err && err.message || err}`));
  }
}
function attachSocket(sock, { relayed } = {}) {
  ws = sock;
  cryptoSession = null;
  let handshakeEph = null;
  if (PAIRED) {
    handshakeEph = workerCrypto.generateEphemeralKeyPair();
    ws.send(JSON.stringify({
      type: "crypto_hello",
      mode: "token",
      machine: MACHINE_UUID,
      pub: workerCrypto.exportPublicKeyRaw(handshakeEph.publicKey).toString("base64")
    }));
  } else {
    onConnectionReady();
  }
  ws.on("message", (data) => {
    if (PAIRED && !cryptoSession) {
      if (!completeClientHandshake(data, handshakeEph)) {
        try {
          ws.close();
        } catch {
        }
      }
      handshakeEph = null;
      return;
    }
    let msg;
    try {
      msg = decodeFrame(data);
    } catch (err) {
      if (cryptoSession) {
        console.error("[daemon] decrypt failed \u2014 dropping connection");
        try {
          ws.close();
        } catch {
        }
        return;
      }
      console.error("[daemon] invalid message:", err.message);
      return;
    }
    handleMessage(msg);
  });
  ws.on("close", () => {
    cryptoSession = null;
    console.log(`[daemon] disconnected, reconnecting in ${reconnectDelay}ms...`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 3e4);
  });
  ws.on("error", (err) => {
    console.error(`[daemon] ws error: ${err.message}`);
  });
}
function completeClientHandshake(data, eph) {
  let hello;
  try {
    hello = JSON.parse(data.toString());
  } catch {
    return false;
  }
  if (hello.type !== "crypto_hello") {
    console.error("[daemon] expected crypto_hello, got", hello.type);
    return false;
  }
  try {
    const hostEphPub = Buffer.from(String(hello.pub || ""), "base64");
    const ephSecret = workerCrypto.computeSharedSecret(eph.privateKey, hostEphPub);
    if (!HOST_STATIC_PUB) throw new Error("no stored host static key \u2014 re-pair required");
    const statSecret = workerCrypto.computeSharedSecret(eph.privateKey, Buffer.from(HOST_STATIC_PUB, "base64"));
    cryptoSession = workerCrypto.createSession(workerCrypto.concatSecrets(ephSecret, statSecret), "", "worker");
    onConnectionReady();
    return true;
  } catch (err) {
    console.error(`[daemon] crypto handshake failed: ${err && err.message || err}`);
    return false;
  }
}
function connect() {
  dialControlConnection().catch((err) => {
    console.error(`[daemon] connect failed: ${err && err.message || err}; retrying in ${reconnectDelay}ms`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 3e4);
  });
}
function dialDirect(host, timeoutMs = 8e3) {
  return new Promise((resolve, reject) => {
    const sock = new WebSocket(`ws://${host}/worker`);
    let settled = false;
    const fail = (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        sock.terminate();
      } catch {
      }
      reject(err);
    };
    const timer = setTimeout(() => fail(new Error(`direct dial to ${host} timed out`)), timeoutMs);
    sock.on("open", () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(sock);
    });
    sock.on("error", (err) => fail(new Error(`direct dial to ${host} failed: ${err && err.message || err}`)));
    sock.on("close", () => fail(new Error(`direct dial to ${host} closed before open`)));
  });
}
function linkWsUrl() {
  let u = (FRONTIER_LINK_URL || "").trim().replace(/\/+$/, "");
  if (u.startsWith("https://")) u = "wss://" + u.slice(8);
  else if (u.startsWith("http://")) u = "ws://" + u.slice(7);
  else if (!u.startsWith("ws://") && !u.startsWith("wss://")) u = "wss://" + u;
  return `${u}/v1/link`;
}
function linkResolve(key) {
  return new Promise((resolve, reject) => {
    const url = linkWsUrl();
    const lws = new WebSocket(url);
    let settled = false;
    const fail = (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        lws.close();
      } catch {
      }
      reject(err);
    };
    const timer = setTimeout(() => fail(new Error("link resolve timed out")), 15e3);
    lws.on("open", () => {
      lws.send(JSON.stringify({ type: "resolve", ...key }));
    });
    lws.on("message", (data) => {
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (msg.type === "found") {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve({ ws: lws, linkId: msg.linkId, candidates: Array.isArray(msg.candidates) ? msg.candidates : [] });
      } else if (msg.type === "error") {
        fail(new Error(`link: ${msg.error}`));
      }
    });
    lws.on("error", (err) => fail(new Error(`link socket error: ${err && err.message || err}`)));
    lws.on("close", (code) => fail(new Error(`link socket closed (${code}) before resolve`)));
  });
}
function linkRelay(lws, linkId) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const onMsg = (data) => {
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (msg.type === "relaying") {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        lws.off("message", onMsg);
        resolve(lws);
      } else if (msg.type === "error") {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(new Error(`link relay: ${msg.error}`));
      }
    };
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        lws.off("message", onMsg);
        reject(new Error("link relay timed out (host never accepted)"));
      }
    }, 15e3);
    lws.on("message", onMsg);
    lws.send(JSON.stringify({ type: "relay", linkId }));
  });
}
async function dialViaCandidatesOrRelay(key, directFirst) {
  if (directFirst) {
    try {
      return { sock: await dialDirect(directFirst), relayed: false };
    } catch (err) {
      console.log(`[daemon] direct ${directFirst} unreachable (${err && err.message || err})`);
    }
  }
  if (!FRONTIER_LINK_URL || FRONTIER_LINK_URL === "off") {
    throw new Error("no direct path and Link is disabled");
  }
  console.log(`[daemon] resolving via Link (${FRONTIER_LINK_URL}) ...`);
  const res = await linkResolve(key);
  for (const cand of res.candidates) {
    if (cand === directFirst) continue;
    try {
      const sock = await dialDirect(cand, 5e3);
      try {
        res.ws.close();
      } catch {
      }
      console.log(`[daemon] reached host directly at ${cand} (via Link discovery)`);
      return { sock, relayed: false };
    } catch (err) {
      console.log(`[daemon] Link candidate ${cand} unreachable`);
    }
  }
  console.log(`[daemon] no reachable candidate \u2014 requesting relay (link ${res.linkId})`);
  const relaySock = await linkRelay(res.ws, res.linkId);
  console.log("[daemon] relay live \u2014 control channel runs end-to-end encrypted over Link");
  return { sock: relaySock, relayed: true };
}
async function dialControlConnection() {
  const linkOn = PAIRED && DAEMON_REACH_ID && FRONTIER_LINK_URL && FRONTIER_LINK_URL !== "off";
  if (!linkOn) {
    if (!EXPLICIT_HOST) throw new Error("no host address and Link is disabled \u2014 nothing to dial");
    console.log(`[daemon] connecting to ws://${EXPLICIT_HOST}/worker...`);
    const sock2 = await dialDirect(EXPLICIT_HOST, 1e4);
    attachSocket(sock2, { relayed: false });
    return;
  }
  console.log(`[daemon] connecting${EXPLICIT_HOST ? ` to ws://${EXPLICIT_HOST}/worker` : " via Link"} (reachId ${DAEMON_REACH_ID.slice(0, 8)}\u2026)...`);
  const { sock, relayed } = await dialViaCandidatesOrRelay({ reachId: DAEMON_REACH_ID }, EXPLICIT_HOST);
  attachSocket(sock, { relayed });
}
async function dialForPairing(code) {
  if (!FRONTIER_LINK_URL || FRONTIER_LINK_URL === "off") {
    if (!EXPLICIT_HOST) throw new Error("no host address and Link is disabled \u2014 cannot pair");
    return dialDirect(EXPLICIT_HOST, 1e4);
  }
  const { sock } = await dialViaCandidatesOrRelay({ code }, EXPLICIT_HOST);
  return sock;
}
function sendHeartbeat() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  if (PAIRED && !cryptoSession) return;
  const inflightArr = [];
  for (const [executionId, entry] of inflight.entries()) {
    inflightArr.push({ executionId, ...entry.meta || {} });
  }
  send({
    type: "heartbeat",
    machine: MACHINE_UUID,
    // Present for paired machines; absent for legacy env-uuid identity (the
    // host accepts that on a trusted network). The host checks it against the
    // stored token hash on every heartbeat once a machine has one.
    token: MACHINE_TOKEN || void 0,
    inflight: inflightArr,
    sourceHash: SOURCE_HASH,
    daemonStartTs: DAEMON_START_TS
  });
}
setInterval(sendHeartbeat, 3e4);
function handleMessage(msg) {
  switch (msg.type) {
    case "execute":
      runExecute(msg).catch((err) => {
        console.error(`[daemon] execute fatal:`, err);
        send({
          type: "execution.result",
          machine: MACHINE_UUID,
          reservationId: msg.reservationId || null,
          executionId: msg.executionId || null,
          outcome: "error",
          error: String(err && err.message || err)
        });
      });
      break;
    case "cancel": {
      const entry = inflight.get(msg.executionId);
      if (entry) {
        try {
          entry.ac.abort();
        } catch {
        }
        console.log(`[daemon] cancel signalled for ${msg.executionId}`);
        sweepDescendantProcesses().catch((err) => {
          console.error(`[daemon] descendant sweep failed: ${err && err.message || err}`);
        });
      } else {
        console.log(`[daemon] cancel for unknown execution ${msg.executionId}`);
      }
      break;
    }
    case "query":
      handleQuery(msg).catch((err) => {
        console.error(`[daemon] query ${msg.queryId} fatal:`, err && err.message || err);
        send({ type: "query.error", queryId: msg.queryId, machine: MACHINE_UUID, error: String(err && err.message || err) });
      });
      break;
    case "update":
      console.log(`[daemon] update requested \u2014 exiting for restart`);
      process.exit(0);
      break;
    case "ext.msg":
      extensionLoader.dispatchWorkerMessage(msg.extension, msg.payload);
      break;
    case "fetch_result":
    case "fetch_chunk":
    case "fetch_end":
      handleFetchResponse(msg);
      break;
    case "mcp_result":
    case "mcp_chunk":
    case "mcp_end":
      handleMcpResponse(msg);
      break;
    default:
      console.log(`[daemon] unknown message type: ${msg.type}`);
  }
}
var pendingFetches = /* @__PURE__ */ new Map();
var fetchSeq = 0;
function handleFetchResponse(msg) {
  const entry = pendingFetches.get(msg.id);
  if (!entry) return;
  if (msg.type === "fetch_result") {
    entry.status = msg.status;
    entry.contentType = msg.contentType;
    entry.total = msg.total || 0;
  } else if (msg.type === "fetch_chunk") {
    entry.chunks[msg.seq] = Buffer.from(String(msg.data || ""), "base64");
  } else if (msg.type === "fetch_end") {
    pendingFetches.delete(msg.id);
    clearTimeout(entry.timer);
    const body = Buffer.concat(entry.chunks.filter(Boolean));
    entry.resolve({ status: entry.status || 0, contentType: entry.contentType || "", body });
  }
}
function wsFetch(pathAndQuery) {
  return new Promise((resolve, reject) => {
    const id = `f${++fetchSeq}`;
    const timer = setTimeout(() => {
      pendingFetches.delete(id);
      reject(new Error(`ws-fetch timed out: ${pathAndQuery}`));
    }, 6e4);
    pendingFetches.set(id, { resolve, reject, chunks: [], timer });
    if (!send({ type: "fetch", id, path: pathAndQuery })) {
      pendingFetches.delete(id);
      clearTimeout(timer);
      reject(new Error("ws-fetch: link down"));
    }
  });
}
async function wsFetchAsset(workerPath) {
  const r = await wsFetch(workerPath);
  if (r.status !== 200) throw new Error(`ws-fetch ${workerPath} -> ${r.status}`);
  return r.body;
}
var pendingMcp = /* @__PURE__ */ new Map();
var mcpSeq = 0;
var mcpBridgeServer = null;
var mcpBridgeUrl = null;
function handleMcpResponse(msg) {
  const entry = pendingMcp.get(msg.id);
  if (!entry) return;
  if (msg.type === "mcp_result") {
    entry.status = msg.status;
    entry.headers = msg.headers && typeof msg.headers === "object" ? msg.headers : {};
  } else if (msg.type === "mcp_chunk") {
    entry.chunks[msg.seq] = Buffer.from(String(msg.data || ""), "base64");
  } else if (msg.type === "mcp_end") {
    pendingMcp.delete(msg.id);
    clearTimeout(entry.timer);
    entry.resolve({ status: entry.status || 0, headers: entry.headers || {}, body: Buffer.concat(entry.chunks.filter(Boolean)) });
  }
}
function wsMcp({ method, headers, body }) {
  return new Promise((resolve, reject) => {
    const id = `m${++mcpSeq}`;
    const timer = setTimeout(() => {
      pendingMcp.delete(id);
      reject(new Error("mcp bridge timed out"));
    }, 12e4);
    pendingMcp.set(id, { resolve, reject, chunks: [], timer });
    if (!send({ type: "mcp_request", id, method, headers, body: body && body.length ? body.toString("base64") : "" })) {
      pendingMcp.delete(id);
      clearTimeout(timer);
      reject(new Error("mcp bridge: link down"));
    }
  });
}
function ensureMcpBridge() {
  if (mcpBridgeUrl) return Promise.resolve(mcpBridgeUrl);
  return new Promise((resolve, reject) => {
    const http = require("http");
    const server = http.createServer((req, res) => {
      const bodyChunks = [];
      req.on("data", (c) => bodyChunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      req.on("end", async () => {
        try {
          const r = await wsMcp({ method: req.method, headers: req.headers, body: Buffer.concat(bodyChunks) });
          res.writeHead(r.status || 502, r.headers || {});
          res.end(r.body);
        } catch (err) {
          if (!res.headersSent) res.writeHead(502, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "mcp bridge failed", detail: String(err && err.message || err) }));
        }
      });
      req.on("error", () => {
        try {
          res.destroy();
        } catch {
        }
      });
    });
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      mcpBridgeServer = server;
      mcpBridgeUrl = `http://127.0.0.1:${addr.port}/mcp`;
      console.log(`[daemon] MCP bridge listening at ${mcpBridgeUrl} (proxied over control WS)`);
      resolve(mcpBridgeUrl);
    });
  });
}
function historyRuntimes() {
  return extensionLoader.allRuntimes().filter((r) => r && r.history);
}
async function delegateSessionGet(queryId, params) {
  for (const rt of historyRuntimes()) {
    let results;
    try {
      results = await rt.history.readSession(params || {});
    } catch (err) {
      console.error(`[daemon][query] readSession failed: ${err && err.message || err}`);
      continue;
    }
    if (results && results.length) {
      const CHUNK = 200;
      for (let i = 0; i < results.length; i += CHUNK) {
        send({ type: "query.result", queryId, machine: MACHINE_UUID, results: results.slice(i, i + CHUNK) });
      }
      send({ type: "query.done", queryId, machine: MACHINE_UUID });
      return;
    }
  }
  send({ type: "query.done", queryId, machine: MACHINE_UUID });
}
async function delegateSessionEntry(queryId, params) {
  for (const rt of historyRuntimes()) {
    let entry = null;
    try {
      entry = await rt.history.readEntry(params || {});
    } catch (err) {
      console.error(`[daemon][query] readEntry failed: ${err && err.message || err}`);
      continue;
    }
    if (entry) {
      send({ type: "query.result", queryId, machine: MACHINE_UUID, results: [entry] });
      send({ type: "query.done", queryId, machine: MACHINE_UUID });
      return;
    }
  }
  send({ type: "query.done", queryId, machine: MACHINE_UUID });
}
async function delegateSessionsSearch(queryId, params) {
  const all = [];
  for (const rt of historyRuntimes()) {
    try {
      const r = await rt.history.searchSessions(params || {});
      if (Array.isArray(r)) all.push(...r);
    } catch (err) {
      console.error(`[daemon][query] searchSessions failed: ${err && err.message || err}`);
    }
  }
  if (all.length) {
    const CHUNK = 25;
    for (let i = 0; i < all.length; i += CHUNK) {
      send({ type: "query.result", queryId, machine: MACHINE_UUID, results: all.slice(i, i + CHUNK) });
    }
  }
  send({ type: "query.done", queryId, machine: MACHINE_UUID });
}
async function delegateSessionDebug(queryId, params) {
  for (const rt of historyRuntimes()) {
    if (typeof rt.history.debugSession !== "function") continue;
    let out = null;
    try {
      out = await rt.history.debugSession(params || {});
    } catch (err) {
      console.error(`[daemon][query] debugSession failed: ${err && err.message || err}`);
      continue;
    }
    if (out) {
      send({ type: "query.result", queryId, machine: MACHINE_UUID, results: [out] });
      send({ type: "query.done", queryId, machine: MACHINE_UUID });
      return;
    }
  }
  send({ type: "query.result", queryId, machine: MACHINE_UUID, results: [{ machine: MACHINE_UUID, sessionId: params && params.sessionId || "", note: "no runtime provides session diagnostics on this worker", entries: [], jsonlMatches: [] }] });
  send({ type: "query.done", queryId, machine: MACHINE_UUID });
}
async function handleQuery(msg) {
  const { queryId, op, params } = msg;
  console.log(`[daemon][query] received ${op} qid=${queryId}`);
  if (op === "session.get") {
    await delegateSessionGet(queryId, params);
    return;
  }
  if (op === "session.debug") {
    await delegateSessionDebug(queryId, params);
    return;
  }
  if (op === "session.entry.get") {
    await delegateSessionEntry(queryId, params);
    return;
  }
  if (op === "sessions.search") {
    await delegateSessionsSearch(queryId, params);
    return;
  }
  if (op === "fs.list" || op === "fs.read" || op === "fs.stat" || op === "fs.list_recursive") {
    await handleFsOp(op, queryId, params || {});
    return;
  }
  if (op === "fs.write") {
    await handleFsWrite(queryId, params || {});
    return;
  }
  if (op === "vcs.status" || op === "vcs.branches" || op === "vcs.log" || op === "vcs.commit" || op === "vcs.diff") {
    await handleVcsOp(op, queryId, params || {});
    return;
  }
  if (op === "worker.exec") {
    await handleWorkerExec(queryId, params || {});
    return;
  }
  send({ type: "query.error", queryId, machine: MACHINE_UUID, error: `unknown op: ${op}` });
}
async function handleWorkerExec(queryId, params) {
  const { workspaceDir, binary, args } = params || {};
  if (typeof binary !== "string" || !binary) {
    return sendFsResult(queryId, { ok: false, code: "bad_request", error: "worker.exec: binary required", stdout: "", stderr: "" });
  }
  const argv = Array.isArray(args) ? args.map((a) => String(a)) : [];
  const ws2 = await validateWorkspace(workspaceDir);
  if (!ws2.ok) {
    return sendFsResult(queryId, { ok: false, code: "bad_workspace", error: ws2.error, stdout: "", stderr: "" });
  }
  const r = await runVcs(ws2.cwd, binary, argv);
  const stdout = r.stdout ? Buffer.isBuffer(r.stdout) ? r.stdout.toString("utf-8") : String(r.stdout) : "";
  const stderr = r.stderr ? String(r.stderr) : "";
  if (!r.ok) {
    return sendFsResult(queryId, {
      ok: false,
      code: r.code || "exec_error",
      error: r.error || "exec failed",
      exitCode: typeof r.exitCode === "number" ? r.exitCode : void 0,
      stdout,
      stderr
    });
  }
  sendFsResult(queryId, { ok: true, stdout, stderr, exitCode: 0 });
}
var FS_LIST_MAX_ENTRIES = 2e3;
var FS_READ_MAX_BYTES = 1048576;
var FS_READ_BINARY_PROBE_BYTES = 8192;
var FS_READ_BINARY_CAP = 256 * 1024;
var FS_LIST_RECURSIVE_MAX_DEPTH = 5;
var FS_LIST_RECURSIVE_MAX_ENTRIES = 5e3;
async function resolveWorkspacePath(workspaceDir, relPath) {
  if (!workspaceDir || typeof workspaceDir !== "string") {
    return { ok: false, error: "workspaceDir is required" };
  }
  const cleaned = String(relPath == null ? "" : relPath).replace(/^\/+/, "");
  const joined = cleaned === "" ? workspaceDir : path.join(workspaceDir, cleaned);
  let rootAbs;
  try {
    rootAbs = await fsp.realpath(workspaceDir);
  } catch {
    return { ok: false, error: `workspace_dir not accessible: ${workspaceDir}` };
  }
  let abs;
  try {
    abs = await fsp.realpath(joined);
  } catch (err) {
    if (err && err.code === "ENOENT") {
      return { ok: false, code: "not_found", error: `not found: ${relPath || "/"}` };
    }
    return { ok: false, error: String(err && err.message || err) };
  }
  const rootWithSep = rootAbs.endsWith(path.sep) ? rootAbs : rootAbs + path.sep;
  if (abs !== rootAbs && !abs.startsWith(rootWithSep)) {
    return { ok: false, code: "path_escape", error: "path escapes workspace_dir" };
  }
  return { ok: true, abs, rootAbs };
}
function sendFsResult(queryId, result) {
  send({ type: "query.result", queryId, machine: MACHINE_UUID, results: [result] });
  send({ type: "query.done", queryId, machine: MACHINE_UUID });
}
async function handleFsOp(op, queryId, params) {
  const { workspaceDir } = params;
  const relPath = params.path;
  const opts = params.opts || {};
  const r = await resolveWorkspacePath(workspaceDir, relPath);
  if (!r.ok) {
    sendFsResult(queryId, { ok: false, error: r.error, code: r.code });
    return;
  }
  if (op === "fs.list") return handleFsList(queryId, r.abs, relPath || "/", opts);
  if (op === "fs.read") return handleFsRead(queryId, r.abs, relPath, opts);
  if (op === "fs.stat") return handleFsStat(queryId, r.abs, relPath);
  if (op === "fs.list_recursive") return handleFsListRecursive(queryId, r.abs, relPath || "/", opts);
}
async function handleFsList(queryId, abs, relPath, opts) {
  let st;
  try {
    st = await fsp.stat(abs);
  } catch (err) {
    return sendFsResult(queryId, { ok: false, error: String(err && err.message || err) });
  }
  if (!st.isDirectory()) {
    return sendFsResult(queryId, { ok: false, code: "not_directory", error: `not a directory: ${relPath}` });
  }
  let names;
  try {
    names = await fsp.readdir(abs);
  } catch (err) {
    return sendFsResult(queryId, { ok: false, error: String(err && err.message || err) });
  }
  names.sort((a, b) => a.localeCompare(b));
  const truncated = names.length > FS_LIST_MAX_ENTRIES;
  const slice = truncated ? names.slice(0, FS_LIST_MAX_ENTRIES) : names;
  const showHidden = opts.show_hidden === true;
  const entries = [];
  for (const name of slice) {
    if (!showHidden && name.startsWith(".")) continue;
    const full = path.join(abs, name);
    let ls;
    try {
      ls = await fsp.lstat(full);
    } catch {
      continue;
    }
    const entry = { name, kind: "other", mtime: Math.floor(ls.mtimeMs) };
    if (ls.isSymbolicLink()) {
      entry.kind = "symlink";
      try {
        entry.symlink_target = await fsp.readlink(full);
      } catch {
      }
    } else if (ls.isDirectory()) {
      entry.kind = "dir";
    } else if (ls.isFile()) {
      entry.kind = "file";
      entry.size = ls.size;
    }
    entries.push(entry);
  }
  sendFsResult(queryId, {
    ok: true,
    path: relPath,
    entries,
    ...truncated ? { truncated: true, total_entries: names.length } : {}
  });
}
async function handleFsRead(queryId, abs, relPath, opts) {
  let st;
  try {
    st = await fsp.stat(abs);
  } catch (err) {
    return sendFsResult(queryId, { ok: false, error: String(err && err.message || err) });
  }
  if (st.isDirectory()) {
    return sendFsResult(queryId, { ok: false, code: "is_directory", error: `is a directory: ${relPath}` });
  }
  if (!st.isFile()) {
    return sendFsResult(queryId, { ok: false, error: `not a regular file: ${relPath}` });
  }
  let buf;
  try {
    buf = await fsp.readFile(abs);
  } catch (err) {
    return sendFsResult(queryId, { ok: false, error: String(err && err.message || err) });
  }
  const probe = buf.subarray(0, FS_READ_BINARY_PROBE_BYTES);
  let isBinary = false;
  for (let i = 0; i < probe.length; i++) {
    if (probe[i] === 0) {
      isBinary = true;
      break;
    }
  }
  if (isBinary) {
    if (buf.length > FS_READ_BINARY_CAP) {
      return sendFsResult(queryId, {
        ok: false,
        code: "binary_too_large",
        error: `binary file too large to preview (${buf.length} bytes, cap ${FS_READ_BINARY_CAP})`
      });
    }
    return sendFsResult(queryId, {
      ok: true,
      path: relPath,
      encoding: "base64",
      content: buf.toString("base64"),
      size: buf.length,
      lines: 0,
      mtime: Math.floor(st.mtimeMs)
    });
  }
  const fullText = buf.toString("utf-8");
  const allLines = fullText.split("\n");
  const totalLines = fullText === "" ? 0 : allLines.length;
  const lineStart = typeof opts.line_start === "number" && opts.line_start >= 1 ? opts.line_start : null;
  const lineEnd = typeof opts.line_end === "number" && opts.line_end >= 1 ? opts.line_end : null;
  let text = fullText;
  let lineRange;
  if (lineStart != null) {
    const endIdx = lineEnd != null ? Math.min(lineEnd, allLines.length) : allLines.length;
    const sliceLines = allLines.slice(lineStart - 1, endIdx);
    text = sliceLines.join("\n");
    lineRange = { start: lineStart, end: endIdx };
  }
  let truncatedAt;
  let outBuf = Buffer.from(text, "utf-8");
  if (outBuf.length > FS_READ_MAX_BYTES) {
    outBuf = outBuf.subarray(0, FS_READ_MAX_BYTES);
    truncatedAt = FS_READ_MAX_BYTES;
    text = outBuf.toString("utf-8");
  }
  sendFsResult(queryId, {
    ok: true,
    path: relPath,
    encoding: "utf-8",
    content: text,
    size: buf.length,
    lines: totalLines,
    mtime: Math.floor(st.mtimeMs),
    ...truncatedAt != null ? { truncated_at: truncatedAt } : {},
    ...lineRange ? { line_range: lineRange } : {}
  });
}
async function handleFsListRecursive(queryId, rootAbs, rootRelPath, opts) {
  const maxDepth = typeof opts.max_depth === "number" && opts.max_depth > 0 ? Math.min(opts.max_depth, FS_LIST_RECURSIVE_MAX_DEPTH) : FS_LIST_RECURSIVE_MAX_DEPTH;
  const maxEntries = typeof opts.max_entries === "number" && opts.max_entries > 0 ? Math.min(opts.max_entries, FS_LIST_RECURSIVE_MAX_ENTRIES) : FS_LIST_RECURSIVE_MAX_ENTRIES;
  const showHidden = opts.show_hidden === true;
  const rootRel = (rootRelPath || "/").replace(/^\/+/, "");
  const queue = [{ abs: rootAbs, rel: rootRel, depth: 0 }];
  const entries = [];
  let truncated = false;
  while (queue.length > 0) {
    if (entries.length >= maxEntries) {
      truncated = true;
      break;
    }
    const { abs, rel, depth } = queue.shift();
    let names;
    try {
      names = await fsp.readdir(abs);
    } catch {
      continue;
    }
    names.sort((a, b) => a.localeCompare(b));
    for (const name of names) {
      if (entries.length >= maxEntries) {
        truncated = true;
        break;
      }
      if (!showHidden && name.startsWith(".")) continue;
      const childAbs = path.join(abs, name);
      const childRel = rel ? `${rel}/${name}` : name;
      let ls;
      try {
        ls = await fsp.lstat(childAbs);
      } catch {
        continue;
      }
      const entry = {
        path: "/" + childRel,
        name,
        kind: "other",
        mtime: Math.floor(ls.mtimeMs)
      };
      if (ls.isSymbolicLink()) {
        entry.kind = "symlink";
      } else if (ls.isDirectory()) {
        entry.kind = "dir";
      } else if (ls.isFile()) {
        entry.kind = "file";
        entry.size = ls.size;
      }
      entries.push(entry);
      if (entry.kind === "dir" && depth + 1 < maxDepth) {
        queue.push({ abs: childAbs, rel: childRel, depth: depth + 1 });
      }
    }
    if (truncated) break;
  }
  sendFsResult(queryId, {
    ok: true,
    path: rootRelPath,
    entries,
    ...truncated ? { truncated: true } : {},
    max_depth: maxDepth,
    max_entries: maxEntries
  });
}
async function handleFsStat(queryId, abs, relPath) {
  let ls;
  try {
    ls = await fsp.lstat(abs);
  } catch (err) {
    return sendFsResult(queryId, { ok: false, error: String(err && err.message || err) });
  }
  let kind, symlinkTarget;
  if (ls.isSymbolicLink()) {
    kind = "symlink";
    try {
      symlinkTarget = await fsp.readlink(abs);
    } catch {
    }
  } else if (ls.isDirectory()) kind = "dir";
  else if (ls.isFile()) kind = "file";
  else kind = "other";
  sendFsResult(queryId, {
    ok: true,
    path: relPath,
    kind,
    size: ls.size,
    mtime: Math.floor(ls.mtimeMs),
    ctime: Math.floor(ls.ctimeMs),
    mode: ls.mode,
    ...symlinkTarget ? { symlink_target: symlinkTarget } : {}
  });
}
var FS_WRITE_MAX_BYTES = 1048576;
var FS_WRITE_AUDIT_DIFF_CAP = 16 * 1024;
function makeUnifiedDiff(beforeText, afterText) {
  if (beforeText === afterText) return { diff: "", diff_truncated: false };
  const a = beforeText.split("\n");
  const b = afterText.split("\n");
  let pre = 0;
  while (pre < a.length && pre < b.length && a[pre] === b[pre]) pre++;
  let suf = 0;
  while (suf < a.length - pre && suf < b.length - pre && a[a.length - 1 - suf] === b[b.length - 1 - suf]) suf++;
  const aMid = a.slice(pre, a.length - suf);
  const bMid = b.slice(pre, b.length - suf);
  const header = `@@ -${pre + 1},${aMid.length} +${pre + 1},${bMid.length} @@`;
  const lines = [header];
  for (const x of aMid) lines.push("-" + x);
  for (const x of bMid) lines.push("+" + x);
  let full = lines.join("\n");
  let truncated = false;
  if (Buffer.byteLength(full, "utf-8") > FS_WRITE_AUDIT_DIFF_CAP) {
    const buf = Buffer.from(full, "utf-8").subarray(0, FS_WRITE_AUDIT_DIFF_CAP);
    full = buf.toString("utf-8");
    truncated = true;
  }
  return { diff: full, diff_truncated: truncated };
}
async function appendAuditLog(workspaceDir, record) {
  const dir = path.join(workspaceDir, ".frontier", "telemetry");
  try {
    await fsp.mkdir(dir, { recursive: true });
    const line = JSON.stringify(record) + "\n";
    await fsp.appendFile(path.join(dir, "fs_writes.jsonl"), line, "utf-8");
  } catch (err) {
    console.error("[daemon][fs.write] audit log append failed:", err && err.message);
  }
}
async function handleFsWrite(queryId, params) {
  const { workspaceDir, content, opts = {}, actor, machine, area } = params || {};
  const relPath = params.path;
  const r = await resolveWorkspacePathForWrite(workspaceDir, relPath);
  if (!r.ok) {
    await appendAuditLog(workspaceDir || "", {
      ts: Date.now(),
      actor: actor || "unknown",
      machine: machine || MACHINE_UUID,
      area: area || "",
      path: relPath || "",
      accepted: false,
      code: r.code,
      error: r.error
    }).catch(() => {
    });
    return sendFsResult(queryId, { ok: false, error: r.error, code: r.code });
  }
  let buf;
  try {
    if (opts.encoding === "base64") {
      buf = Buffer.from(String(content || ""), "base64");
    } else {
      buf = Buffer.from(String(content || ""), "utf-8");
    }
  } catch (err) {
    return sendFsResult(queryId, { ok: false, error: `failed to decode content: ${err && err.message}` });
  }
  if (buf.length > FS_WRITE_MAX_BYTES) {
    return sendFsResult(queryId, { ok: false, code: "too_large", error: `content exceeds 1 MB cap (${buf.length} bytes)` });
  }
  let beforeBuf = null;
  let priorMtime = null;
  try {
    beforeBuf = await fsp.readFile(r.abs);
    const st = await fsp.stat(r.abs);
    priorMtime = Math.floor(st.mtimeMs);
  } catch (err) {
    if (err && err.code !== "ENOENT") {
      return sendFsResult(queryId, { ok: false, error: String(err.message || err) });
    }
    if (opts.create_if_missing === false) {
      return sendFsResult(queryId, { ok: false, code: "not_found", error: "target does not exist and create_if_missing is false" });
    }
  }
  if (typeof opts.expected_mtime === "number" && priorMtime !== null) {
    if (opts.expected_mtime !== priorMtime) {
      return sendFsResult(queryId, {
        ok: false,
        code: "mtime_mismatch",
        error: "file changed on disk since you opened it",
        actual_mtime: priorMtime
      });
    }
  }
  const dir = path.dirname(r.abs);
  const base = path.basename(r.abs);
  const tmpPath = path.join(dir, `.${base}.tmp.${process.pid}.${crypto.randomBytes(4).toString("hex")}`);
  try {
    await fsp.mkdir(dir, { recursive: true });
    let mode;
    try {
      mode = (await fsp.stat(r.abs)).mode;
    } catch {
    }
    await fsp.writeFile(tmpPath, buf);
    if (typeof mode === "number") {
      try {
        await fsp.chmod(tmpPath, mode);
      } catch {
      }
    }
    await fsp.rename(tmpPath, r.abs);
  } catch (err) {
    try {
      await fsp.unlink(tmpPath);
    } catch {
    }
    return sendFsResult(queryId, { ok: false, error: String(err.message || err) });
  }
  let newMtime = Date.now();
  try {
    newMtime = Math.floor((await fsp.stat(r.abs)).mtimeMs);
  } catch {
  }
  const beforeText = beforeBuf ? beforeBuf.toString("utf-8") : "";
  const afterText = buf.toString("utf-8");
  const { diff, diff_truncated } = makeUnifiedDiff(beforeText, afterText);
  await appendAuditLog(workspaceDir, {
    ts: Date.now(),
    actor: actor || "unknown",
    machine: machine || MACHINE_UUID,
    area: area || "",
    path: relPath,
    accepted: true,
    before_bytes: beforeBuf ? beforeBuf.length : 0,
    after_bytes: buf.length,
    before_sha256: beforeBuf ? crypto.createHash("sha256").update(beforeBuf).digest("hex") : null,
    after_sha256: crypto.createHash("sha256").update(buf).digest("hex"),
    diff_unified: diff,
    diff_truncated
  });
  sendFsResult(queryId, {
    ok: true,
    path: relPath,
    bytes: buf.length,
    mtime: newMtime
  });
}
async function resolveWorkspacePathForWrite(workspaceDir, relPath) {
  if (!workspaceDir || typeof workspaceDir !== "string") {
    return { ok: false, error: "workspaceDir is required" };
  }
  if (!relPath || typeof relPath !== "string" || relPath === "/" || relPath === "") {
    return { ok: false, code: "path_escape", error: "cannot write to workspace root" };
  }
  const cleaned = relPath.replace(/^\/+/, "");
  let rootAbs;
  try {
    rootAbs = await fsp.realpath(workspaceDir);
  } catch {
    return { ok: false, error: `workspace_dir not accessible: ${workspaceDir}` };
  }
  const joined = path.join(workspaceDir, cleaned);
  const parent = path.dirname(joined);
  const base = path.basename(joined);
  let parentAbs;
  try {
    parentAbs = await fsp.realpath(parent);
  } catch (err) {
    if (err && err.code === "ENOENT") {
      let cursor = parent;
      while (cursor !== path.dirname(cursor)) {
        try {
          const real = await fsp.realpath(cursor);
          const rootWithSep2 = rootAbs.endsWith(path.sep) ? rootAbs : rootAbs + path.sep;
          if (real !== rootAbs && !real.startsWith(rootWithSep2)) {
            return { ok: false, code: "path_escape", error: "path escapes workspace_dir" };
          }
          return { ok: true, abs: joined };
        } catch {
        }
        cursor = path.dirname(cursor);
      }
      return { ok: false, code: "path_escape", error: "path escapes workspace_dir" };
    }
    return { ok: false, error: String(err && err.message || err) };
  }
  const rootWithSep = rootAbs.endsWith(path.sep) ? rootAbs : rootAbs + path.sep;
  if (parentAbs !== rootAbs && !parentAbs.startsWith(rootWithSep)) {
    return { ok: false, code: "path_escape", error: "path escapes workspace_dir" };
  }
  return { ok: true, abs: path.join(parentAbs, base) };
}
var VCS_DETECT_TTL_MS = 6e4;
var VCS_EXEC_TIMEOUT_MS = 1e4;
var VCS_STATUS_MAX_ENTRIES = 2e3;
var VCS_STDERR_CAP_BYTES = 4096;
var vcsDetectCache = /* @__PURE__ */ new Map();
async function vcsDetect(workspaceDir) {
  if (!workspaceDir) return { vcs: null, reason: "no_workspace_dir" };
  const cached = vcsDetectCache.get(workspaceDir);
  if (cached && Date.now() - cached.ts < VCS_DETECT_TTL_MS) {
    return {
      vcs: cached.vcs,
      reason: cached.vcs ? null : "no_vcs",
      repoRoot: cached.repoRoot || workspaceDir
    };
  }
  let vcs = null;
  let repoRoot = null;
  let cursor = path.resolve(workspaceDir);
  while (true) {
    try {
      await fsp.stat(path.join(cursor, ".git"));
      vcs = "git";
      repoRoot = cursor;
      break;
    } catch {
    }
    try {
      await fsp.stat(path.join(cursor, ".hg"));
      vcs = "hg";
      repoRoot = cursor;
      break;
    } catch {
    }
    const parent = path.dirname(cursor);
    if (parent === cursor) break;
    cursor = parent;
  }
  vcsDetectCache.set(workspaceDir, { vcs, repoRoot, ts: Date.now() });
  return {
    vcs,
    reason: vcs ? null : "no_vcs",
    repoRoot: repoRoot || workspaceDir
  };
}
function runVcs(workspaceDir, binary, args, opts = {}) {
  const { execFile } = require("child_process");
  return new Promise((resolve) => {
    execFile(binary, args, {
      cwd: workspaceDir,
      timeout: opts.timeout || VCS_EXEC_TIMEOUT_MS,
      maxBuffer: opts.maxBuffer || 8 * 1024 * 1024,
      windowsHide: true,
      env: { ...process.env, LANG: "C", LC_ALL: "C" }
    }, (err, stdout, stderr) => {
      const stderrText = (stderr || "").toString("utf-8").slice(0, VCS_STDERR_CAP_BYTES);
      if (err) {
        return resolve({
          ok: false,
          code: "vcs_error",
          error: stderrText || String(err.message || err),
          exitCode: err.code
        });
      }
      resolve({ ok: true, stdout, stderr: stderrText });
    });
  });
}
function parseGitStatusZ(stdout) {
  const text = stdout.toString("utf-8");
  const parts = text.split("\0");
  if (parts[parts.length - 1] === "") parts.pop();
  let branch = null;
  let detached = false;
  let upstream = null;
  let ahead = 0;
  let behind = 0;
  const entries = [];
  let i = 0;
  if (parts[0] && parts[0].startsWith("##")) {
    const header = parts[0].slice(2).trim();
    if (header.startsWith("HEAD (no branch)") || header.startsWith("No commits yet")) {
      detached = header.startsWith("HEAD");
      branch = null;
    } else {
      const bracketIdx = header.indexOf(" [");
      const beforeBracket = bracketIdx >= 0 ? header.slice(0, bracketIdx) : header;
      const dotIdx = beforeBracket.indexOf("...");
      if (dotIdx >= 0) {
        branch = beforeBracket.slice(0, dotIdx);
        upstream = beforeBracket.slice(dotIdx + 3);
      } else {
        branch = beforeBracket;
      }
      if (bracketIdx >= 0) {
        const bracket = header.slice(bracketIdx + 2, header.length - 1);
        const aheadMatch = bracket.match(/ahead (\d+)/);
        const behindMatch = bracket.match(/behind (\d+)/);
        if (aheadMatch) ahead = parseInt(aheadMatch[1], 10) || 0;
        if (behindMatch) behind = parseInt(behindMatch[1], 10) || 0;
      }
    }
    i = 1;
  }
  while (i < parts.length && entries.length < VCS_STATUS_MAX_ENTRIES) {
    const rec = parts[i++];
    if (!rec || rec.length < 3) continue;
    const index = rec[0];
    const worktree = rec[1];
    const filePath = rec.slice(3);
    const entry = { path: filePath, index, worktree };
    if (index === "R" || index === "C" || worktree === "R" || worktree === "C") {
      const origin = parts[i++];
      if (origin) entry.rename_from = origin;
    }
    entries.push(entry);
  }
  const truncated = entries.length >= VCS_STATUS_MAX_ENTRIES;
  return { branch, detached, upstream, ahead, behind, entries, truncated };
}
function parseHgStatus(stdout) {
  const text = stdout.toString("utf-8");
  const entries = [];
  for (const rawLine of text.split("\n")) {
    if (entries.length >= VCS_STATUS_MAX_ENTRIES) break;
    if (!rawLine) continue;
    const letter = rawLine[0];
    const filePath = rawLine.slice(2);
    entries.push({ path: filePath, index: " ", worktree: letter });
  }
  const truncated = entries.length >= VCS_STATUS_MAX_ENTRIES;
  return { entries, truncated };
}
async function handleVcsStatus(queryId, params) {
  var _a;
  let { workspaceDir } = params || {};
  const detect = await vcsDetect(workspaceDir);
  if (detect.repoRoot) workspaceDir = detect.repoRoot;
  if (!detect.vcs) {
    return sendFsResult(queryId, {
      ok: false,
      code: "not_a_repo",
      error: `${workspaceDir}: not a git or mercurial repository (no .git or .hg at workspace_dir)`
    });
  }
  if (detect.vcs === "git") {
    const r = await runVcs(workspaceDir, "git", [
      "status",
      "--porcelain=v1",
      "--branch",
      "-z",
      ...((_a = params == null ? void 0 : params.opts) == null ? void 0 : _a.include_ignored) ? ["--ignored=traditional"] : [],
      "--untracked-files=normal"
    ]);
    if (!r.ok) return sendFsResult(queryId, r);
    const parsed2 = parseGitStatusZ(r.stdout);
    return sendFsResult(queryId, {
      ok: true,
      vcs: "git",
      branch: parsed2.branch,
      detached: parsed2.detached,
      upstream: parsed2.upstream,
      ahead: parsed2.ahead,
      behind: parsed2.behind,
      entries: parsed2.entries,
      clean: parsed2.entries.length === 0,
      ...parsed2.truncated ? { truncated: true } : {}
    });
  }
  const branchR = await runVcs(workspaceDir, "hg", ["branch"]);
  if (!branchR.ok) return sendFsResult(queryId, branchR);
  const branch = branchR.stdout.toString("utf-8").trim() || null;
  const statusR = await runVcs(workspaceDir, "hg", ["status"]);
  if (!statusR.ok) return sendFsResult(queryId, statusR);
  const parsed = parseHgStatus(statusR.stdout);
  return sendFsResult(queryId, {
    ok: true,
    vcs: "hg",
    branch,
    detached: false,
    upstream: null,
    ahead: 0,
    behind: 0,
    entries: parsed.entries,
    clean: parsed.entries.length === 0,
    ...parsed.truncated ? { truncated: true } : {}
  });
}
var VCS_BRANCHES_MAX = 1e3;
var VCS_MAINLINE_NAMES = /* @__PURE__ */ new Set(["main", "master", "trunk", "default"]);
var VCS_MAINLINE_CANDIDATES = [
  "refs/heads/main",
  "refs/heads/master",
  "refs/heads/trunk",
  "refs/remotes/origin/main",
  "refs/remotes/origin/master",
  "refs/remotes/origin/trunk"
];
function isMainlineBranchName(name) {
  if (!name) return false;
  if (VCS_MAINLINE_NAMES.has(name)) return true;
  const lastSeg = name.split("/").pop();
  return lastSeg ? VCS_MAINLINE_NAMES.has(lastSeg) : false;
}
async function getExistingMainlineRefs(workspaceDir) {
  const r = await runVcs(workspaceDir, "git", [
    "for-each-ref",
    "--format=%(refname:short)",
    ...VCS_MAINLINE_CANDIDATES
  ]);
  if (!r.ok) return [];
  return r.stdout.toString("utf-8").split("\n").map((s) => s.trim()).filter(Boolean);
}
async function handleVcsBranches(queryId, params) {
  let { workspaceDir } = params || {};
  const opts = (params == null ? void 0 : params.opts) || {};
  const includeRemote = opts.include_remote === true;
  const detect = await vcsDetect(workspaceDir);
  if (detect.repoRoot) workspaceDir = detect.repoRoot;
  if (!detect.vcs) {
    return sendFsResult(queryId, { ok: false, code: "not_a_repo", error: "not a repo" });
  }
  if (detect.vcs === "git") {
    const refs = ["refs/heads", ...includeRemote ? ["refs/remotes"] : []];
    const r2 = await runVcs(workspaceDir, "git", [
      "for-each-ref",
      "--format=%(refname:short)%(objectname)%(committerdate:unix)%(contents:subject)%(upstream:short)%(upstream:track)",
      "--sort=-committerdate",
      ...refs
    ]);
    if (!r2.ok) return sendFsResult(queryId, r2);
    const headR = await runVcs(workspaceDir, "git", ["symbolic-ref", "--short", "-q", "HEAD"]);
    const current = headR.ok ? headR.stdout.toString("utf-8").trim() || null : null;
    const text2 = r2.stdout.toString("utf-8");
    const records2 = text2.split("\n");
    while (records2.length && records2[records2.length - 1] === "") records2.pop();
    const branches2 = [];
    for (const rec of records2) {
      if (branches2.length >= VCS_BRANCHES_MAX) break;
      if (!rec) continue;
      const parts = rec.split("");
      if (parts.length < 6) continue;
      const [name, hash, tsStr, subject, upstream, track] = parts;
      const isRemote = name.startsWith("origin/") || name.includes("/") && refs.includes("refs/remotes");
      let ahead = 0, behind = 0;
      if (track) {
        const a = track.match(/ahead (\d+)/);
        const b = track.match(/behind (\d+)/);
        if (a) ahead = parseInt(a[1], 10) || 0;
        if (b) behind = parseInt(b[1], 10) || 0;
      }
      const mainline = isMainlineBranchName(name);
      branches2.push({
        name,
        remote: !!isRemote,
        current: name === current,
        mainline,
        tip_hash: hash,
        tip_ts: parseInt(tsStr, 10) || 0,
        tip_subject: subject,
        upstream: upstream || null,
        ahead,
        behind,
        unique_count: null
        // filled in below for non-mainline
      });
    }
    const mainlineRefs = await getExistingMainlineRefs(workspaceDir);
    if (mainlineRefs.length > 0) {
      const counts = await Promise.all(branches2.map(async (b) => {
        if (b.mainline) return null;
        const cr = await runVcs(workspaceDir, "git", ["rev-list", "--count", b.name, "--not", ...mainlineRefs]);
        if (!cr.ok) return null;
        return parseInt(cr.stdout.toString("utf-8").trim(), 10) || 0;
      }));
      for (let i = 0; i < branches2.length; i++) branches2[i].unique_count = counts[i];
    }
    const filtered = branches2.filter((b) => b.mainline || b.unique_count == null || b.unique_count > 0);
    return sendFsResult(queryId, { ok: true, vcs: "git", branches: filtered, current });
  }
  const tpl = "{branch}\\x1f{rev}\\x1f{node}\\x1f{p1node}\\x1f{date|hgdate}\\x1f{desc|firstline}\\n";
  const VCS_BRANCHES_LIMIT = String(VCS_BRANCHES_MAX);
  const isUnknownCmd = (err) => /unknown command|no such command|unknown revset/i.test(err || "");
  let r = await runVcs(workspaceDir, "hg", ["log", "-r", "reverse(head() and draft())", "--template", tpl, "--limit", VCS_BRANCHES_LIMIT]);
  if (!r.ok && isUnknownCmd(r.error)) {
    r = await runVcs(workspaceDir, "hg", ["log", "-r", "reverse(head() and not closed())", "--template", tpl, "--limit", VCS_BRANCHES_LIMIT]);
  }
  if (!r.ok && isUnknownCmd(r.error)) {
    r = await runVcs(workspaceDir, "hg", ["branches", "--template", tpl]);
  }
  if (!r.ok) return sendFsResult(queryId, r);
  const wdpR = await runVcs(workspaceDir, "hg", ["log", "-r", ".", "--template", "{node}\\n"]);
  const workingDirParent = wdpR.ok ? wdpR.stdout.toString("utf-8").trim() || null : null;
  const text = r.stdout.toString("utf-8");
  const records = text.split("\n");
  while (records.length && records[records.length - 1] === "") records.pop();
  const branches = [];
  const seenHashes = /* @__PURE__ */ new Set();
  for (const rec of records) {
    if (branches.length >= VCS_BRANCHES_MAX) break;
    if (!rec) continue;
    const parts = rec.split("");
    if (parts.length < 6) continue;
    const branchName = (parts[0] || "").trim();
    const hash = (parts[2] || "").trim();
    const parentHash = (parts[3] || "").trim();
    const hgdate = parts[4] || "";
    const subject = (parts[5] || "").trim();
    if (!hash) continue;
    if (seenHashes.has(hash)) continue;
    seenHashes.add(hash);
    const displayName = branchName && branchName !== "default" ? branchName : subject || hash.slice(0, 12);
    const ts = parseInt((hgdate || "").split(" ")[0], 10) || 0;
    branches.push({
      // `name` is the user-facing display; the UI calls vcs.log with
      // `tip_hash` as the branch identifier (see handleVcsLog), so the
      // display can be arbitrary text without breaking the revset.
      name: displayName,
      remote: false,
      // Draft heads aren't mainline by definition — they're per-stack
      // tips. The working-dir parent's hash determines "current".
      current: workingDirParent != null && hash === workingDirParent,
      mainline: false,
      tip_hash: hash,
      // Surface the parent hash so a future iteration can render
      // the actual stack/tree structure on the client side.
      parent_hash: parentHash || null,
      tip_ts: ts,
      tip_subject: subject,
      upstream: null,
      ahead: 0,
      behind: 0,
      unique_count: null
      // not computed for hg in v1
    });
  }
  const curBranchR = await runVcs(workspaceDir, "hg", ["branch"]);
  const currentBranchName = curBranchR.ok ? curBranchR.stdout.toString("utf-8").trim() || null : null;
  return sendFsResult(queryId, { ok: true, vcs: "hg", branches, current: currentBranchName });
}
var VCS_LOG_DEFAULT_LIMIT = 50;
var VCS_LOG_MAX_LIMIT = 500;
async function handleVcsLog(queryId, params) {
  let { workspaceDir } = params || {};
  const opts = (params == null ? void 0 : params.opts) || {};
  const detect = await vcsDetect(workspaceDir);
  if (detect.repoRoot) workspaceDir = detect.repoRoot;
  if (!detect.vcs) {
    return sendFsResult(queryId, { ok: false, code: "not_a_repo", error: "not a repo" });
  }
  const branch = typeof opts.branch === "string" && opts.branch ? opts.branch : null;
  const mode = opts.mode === "branch_full" ? "branch_full" : opts.mode === "mainline" ? "mainline" : "branch_unique";
  const limit = Math.min(Math.max(parseInt(opts.limit, 10) || VCS_LOG_DEFAULT_LIMIT, 1), VCS_LOG_MAX_LIMIT);
  const skip = Math.max(parseInt(opts.skip, 10) || 0, 0);
  const rawSearch = typeof opts.search === "string" ? opts.search.trim() : "";
  const search = rawSearch.length > 0 && rawSearch.length <= 200 ? rawSearch : null;
  if (detect.vcs === "git") {
    const args2 = [
      "log",
      "-z",
      `--max-count=${limit}`,
      `--skip=${skip}`,
      // %(trailers:...) pulls trailer-line values out of the commit
      // body; valueonly + key= scopes to the Frontier tagging trailer
      // workers add per structured_start.md instructions. Multiple
      // trailers on one commit are newline-separated in the value.
      "--pretty=format:%H%x1f%h%x1f%P%x1f%an%x1f%ae%x1f%at%x1f%s%x1f%D%x1f%(trailers:key=Frontier-Space-UUID,valueonly,separator=%x20)"
    ];
    if (search) {
      args2.push("--fixed-strings", "-i", `--grep=${search}`);
    }
    let resolvedBranch = branch;
    if (mode === "mainline") {
      const mainlineRefs = await getExistingMainlineRefs(workspaceDir);
      resolvedBranch = mainlineRefs[0] || branch || "HEAD";
    }
    if (resolvedBranch) {
      args2.push(resolvedBranch);
      if (mode === "branch_unique" && !isMainlineBranchName(resolvedBranch)) {
        const mainlineRefs = await getExistingMainlineRefs(workspaceDir);
        if (mainlineRefs.length > 0) args2.push("--not", ...mainlineRefs);
      }
    }
    const r2 = await runVcs(workspaceDir, "git", args2);
    if (!r2.ok) return sendFsResult(queryId, r2);
    const text2 = r2.stdout.toString("utf-8");
    const records2 = text2.split("\0");
    if (records2[records2.length - 1] === "") records2.pop();
    const commits2 = [];
    for (const rec of records2) {
      if (!rec) continue;
      const parts = rec.split("");
      if (parts.length < 8) continue;
      const [hash, shortHash, parentsStr, an, ae, atStr, subject, refsStr, trailerStr] = parts;
      const spaceUid = trailerStr ? trailerStr.split(/\s+/).find((s) => s.trim()) || null : null;
      commits2.push({
        hash,
        short_hash: shortHash,
        parents: parentsStr ? parentsStr.split(" ").filter(Boolean) : [],
        author_name: an,
        author_email: ae,
        author_ts: parseInt(atStr, 10) || 0,
        subject,
        refs: refsStr ? refsStr.split(",").map((s) => s.trim()).filter(Boolean) : [],
        space_uid: spaceUid
      });
    }
    return sendFsResult(queryId, {
      ok: true,
      vcs: "git",
      commits: commits2,
      has_more: commits2.length === limit,
      branch: branch || null,
      mode
    });
  }
  const tplFields = "{node}\\x1f{node|short}\\x1f{p1node} {p2node}\\x1f{author|person}\\x1f{author|email}\\x1f{date|hgdate}\\x1f{desc|firstline}\\x1f{bookmarks} {tags}\\x1f{desc|nonempty}\\x00";
  const args = ["log", "--template", tplFields];
  if (mode === "mainline") {
    let chosen = "default";
    for (const cand of ["default", "main", "master"]) {
      const probe = await runVcs(workspaceDir, "hg", ["log", "-b", cand, "-l", "1", "--template", "{node}"]);
      if (probe.ok && probe.stdout.toString("utf-8").trim()) {
        chosen = cand;
        break;
      }
    }
    args.push("-b", chosen);
  } else if (branch) {
    if (mode === "branch_unique") {
      const safe = branch.replace(/'/g, "''");
      args.push("-r", `reverse(ancestors('${safe}') and draft())`);
    } else {
      args.push("-b", branch);
    }
  }
  if (search) {
    const safeSearch = search.replace(/'/g, "''");
    args.push("-r", `keyword('${safeSearch}')`);
  }
  const fetchLimit = branch && mode === "branch_unique" ? 200 : limit + skip;
  args.push("-l", String(fetchLimit));
  const r = await runVcs(workspaceDir, "hg", args);
  if (!r.ok) return sendFsResult(queryId, r);
  const text = r.stdout.toString("utf-8");
  const records = text.split("\0");
  if (records[records.length - 1] === "") records.pop();
  const commits = [];
  const trailerRe = /^Frontier-Space-UUID:\s*([0-9a-fA-F-]+)\s*$/m;
  for (const rec of records.slice(skip)) {
    if (!rec) continue;
    const parts = rec.split("");
    if (parts.length < 8) continue;
    const [hash, shortHash, parentsStr, an, ae, hgdate, subject, refsStr, fullDesc] = parts;
    const parents = parentsStr.split(" ").filter((p) => p && !/^0+$/.test(p));
    const m = fullDesc ? trailerRe.exec(fullDesc) : null;
    commits.push({
      hash,
      short_hash: shortHash,
      parents,
      author_name: an,
      author_email: ae,
      author_ts: parseInt((hgdate || "").split(" ")[0], 10) || 0,
      subject,
      refs: refsStr ? refsStr.split(/\s+/).filter(Boolean) : [],
      space_uid: m ? m[1] : null
    });
  }
  return sendFsResult(queryId, {
    ok: true,
    vcs: "hg",
    commits,
    has_more: commits.length === limit,
    branch: branch || null,
    mode
  });
}
async function handleVcsCommit(queryId, params) {
  let { workspaceDir } = params || {};
  const rev = typeof (params == null ? void 0 : params.rev) === "string" ? params.rev : "";
  if (!rev) {
    return sendFsResult(queryId, { ok: false, code: "vcs_error", error: "rev is required" });
  }
  const detect = await vcsDetect(workspaceDir);
  if (detect.repoRoot) workspaceDir = detect.repoRoot;
  if (!detect.vcs) {
    return sendFsResult(queryId, { ok: false, code: "not_a_repo", error: "not a repo" });
  }
  if (detect.vcs === "git") {
    const fmt = "%H%x1f%h%x1f%P%x1f%an%x1f%ae%x1f%at%x1f%s%x1f%b%x00";
    const r = await runVcs(workspaceDir, "git", ["show", `--pretty=format:${fmt}`, "--name-status", "-z", rev]);
    if (!r.ok) return sendFsResult(queryId, r);
    const text = r.stdout.toString("utf-8");
    const split = text.split("\0");
    const headerRec = split.shift() || "";
    const headerParts2 = headerRec.split("");
    if (headerParts2.length < 8) {
      return sendFsResult(queryId, { ok: false, code: "vcs_error", error: "unexpected git show output" });
    }
    const [hash2, shortHash2, parentsStr2, an2, ae2, atStr, subject2, body2] = headerParts2;
    while (split.length && split[0] === "") split.shift();
    const files2 = [];
    while (split.length) {
      let code = split.shift();
      if (code === "" || code === void 0) continue;
      code = code.replace(/^\s+/, "");
      if (!code) continue;
      const letter = code[0];
      if (letter === "R" || letter === "C") {
        const from = split.shift() || "";
        const to = split.shift() || "";
        files2.push({ path: to, status: letter, rename_from: from });
      } else {
        const p = split.shift() || "";
        if (!p) continue;
        files2.push({ path: p, status: letter });
      }
    }
    return sendFsResult(queryId, {
      ok: true,
      vcs: "git",
      hash: hash2,
      short_hash: shortHash2,
      parents: parentsStr2 ? parentsStr2.split(" ").filter(Boolean) : [],
      author_name: an2,
      author_email: ae2,
      author_ts: parseInt(atStr, 10) || 0,
      subject: subject2,
      body: body2 || "",
      files: files2
    });
  }
  const tpl = "{node}\\x1f{node|short}\\x1f{p1node} {p2node}\\x1f{author|person}\\x1f{author|email}\\x1f{date|hgdate}\\x1f{desc}\\x00";
  const headerR = await runVcs(workspaceDir, "hg", ["log", "-r", rev, "--template", tpl]);
  if (!headerR.ok) return sendFsResult(queryId, headerR);
  const headerParts = headerR.stdout.toString("utf-8").replace(/\0$/, "").split("");
  if (headerParts.length < 7) {
    return sendFsResult(queryId, { ok: false, code: "vcs_error", error: "unexpected hg log output" });
  }
  const [hash, shortHash, parentsStr, an, ae, hgdate, fullDesc] = headerParts;
  const firstNewline = (fullDesc || "").indexOf("\n");
  const subject = firstNewline === -1 ? fullDesc || "" : fullDesc.slice(0, firstNewline);
  const body = firstNewline === -1 ? "" : fullDesc.slice(firstNewline + 1);
  const parents = parentsStr.split(" ").filter((p) => p && !/^0+$/.test(p));
  const filesR = await runVcs(workspaceDir, "hg", ["status", "--change", rev]);
  const files = [];
  if (filesR.ok) {
    for (const line of filesR.stdout.toString("utf-8").split("\n")) {
      if (!line) continue;
      const letter = line[0];
      const p = line.slice(2);
      files.push({ path: p, status: letter === "R" ? "D" : letter });
    }
  }
  return sendFsResult(queryId, {
    ok: true,
    vcs: "hg",
    hash,
    short_hash: shortHash,
    parents,
    author_name: an,
    author_email: ae,
    author_ts: parseInt((hgdate || "").split(" ")[0], 10) || 0,
    subject,
    body: body || "",
    files
  });
}
var VCS_DIFF_SIDE_MAX_BYTES = 1048576;
var VCS_DIFF_UNIFIED_MAX_BYTES = 2 * 1048576;
var VCS_DIFF_BINARY_PROBE = 8192;
function isLikelyBinary(buf) {
  if (!buf || !buf.length) return false;
  const slice = buf.subarray(0, Math.min(buf.length, VCS_DIFF_BINARY_PROBE));
  for (let i = 0; i < slice.length; i++) if (slice[i] === 0) return true;
  return false;
}
async function handleVcsDiff(queryId, params) {
  var _a;
  let { workspaceDir } = params || {};
  const rev = typeof (params == null ? void 0 : params.rev) === "string" ? params.rev : "";
  const file = typeof (params == null ? void 0 : params.file) === "string" ? params.file : "";
  const mode = ((_a = params == null ? void 0 : params.opts) == null ? void 0 : _a.mode) === "worktree_vs_head" ? "worktree_vs_head" : "commit";
  if (!file) {
    return sendFsResult(queryId, { ok: false, code: "vcs_error", error: "file is required" });
  }
  if (mode === "commit" && !rev) {
    return sendFsResult(queryId, { ok: false, code: "vcs_error", error: "rev is required for commit-mode diff" });
  }
  const detect = await vcsDetect(workspaceDir);
  if (detect.repoRoot) workspaceDir = detect.repoRoot;
  if (!detect.vcs) {
    return sendFsResult(queryId, { ok: false, code: "not_a_repo", error: "not a repo" });
  }
  if (mode === "worktree_vs_head") {
    const absPath = path.join(workspaceDir, file);
    let afterBuf2 = Buffer.alloc(0);
    try {
      afterBuf2 = await fsp.readFile(absPath);
    } catch (err) {
      if (err && err.code !== "ENOENT") {
        return sendFsResult(queryId, { ok: false, code: "vcs_error", error: String(err.message || err) });
      }
    }
    let beforeBuf2 = Buffer.alloc(0);
    if (detect.vcs === "git") {
      const r = await runVcs(workspaceDir, "git", ["show", `HEAD:${file}`], { maxBuffer: VCS_DIFF_SIDE_MAX_BYTES * 2 });
      if (r.ok) beforeBuf2 = Buffer.from(r.stdout);
    } else {
      const r = await runVcs(workspaceDir, "hg", ["cat", "-r", ".", file], { maxBuffer: VCS_DIFF_SIDE_MAX_BYTES * 2 });
      if (r.ok) beforeBuf2 = Buffer.from(r.stdout);
    }
    if (isLikelyBinary(beforeBuf2) || isLikelyBinary(afterBuf2)) {
      return sendFsResult(queryId, { ok: false, code: "binary", error: "binary file" });
    }
    if (beforeBuf2.length > VCS_DIFF_SIDE_MAX_BYTES || afterBuf2.length > VCS_DIFF_SIDE_MAX_BYTES) {
      return sendFsResult(queryId, { ok: false, code: "too_large", error: "file exceeds 1 MB cap on one or both sides" });
    }
    const unifiedArgs = detect.vcs === "git" ? ["diff", "HEAD", "--", file] : ["diff", file];
    const unifiedR2 = await runVcs(workspaceDir, detect.vcs, unifiedArgs, { maxBuffer: VCS_DIFF_UNIFIED_MAX_BYTES });
    const unified2 = unifiedR2.ok ? unifiedR2.stdout.toString("utf-8").slice(0, VCS_DIFF_UNIFIED_MAX_BYTES) : "";
    return sendFsResult(queryId, {
      ok: true,
      vcs: detect.vcs,
      file,
      sides: {
        before: beforeBuf2.toString("utf-8"),
        after: afterBuf2.toString("utf-8"),
        before_bytes: beforeBuf2.length,
        after_bytes: afterBuf2.length
      },
      unified: unified2
    });
  }
  if (detect.vcs === "git") {
    const showAt = async (spec) => {
      const r = await runVcs(workspaceDir, "git", ["show", spec], { maxBuffer: VCS_DIFF_SIDE_MAX_BYTES * 2 });
      return r;
    };
    const afterR2 = await showAt(`${rev}:${file}`);
    if (!afterR2.ok) return sendFsResult(queryId, afterR2);
    const beforeR2 = await showAt(`${rev}^:${file}`);
    const beforeBuf2 = beforeR2.ok ? Buffer.from(beforeR2.stdout) : Buffer.alloc(0);
    const afterBuf2 = Buffer.from(afterR2.stdout);
    if (isLikelyBinary(beforeBuf2) || isLikelyBinary(afterBuf2)) {
      return sendFsResult(queryId, { ok: false, code: "binary", error: "binary file" });
    }
    const truncated = beforeBuf2.length > VCS_DIFF_SIDE_MAX_BYTES || afterBuf2.length > VCS_DIFF_SIDE_MAX_BYTES;
    if (truncated) {
      return sendFsResult(queryId, { ok: false, code: "too_large", error: "file exceeds 1 MB cap on one or both sides" });
    }
    const unifiedR2 = await runVcs(workspaceDir, "git", ["diff", `${rev}^!`, "--", file], { maxBuffer: VCS_DIFF_UNIFIED_MAX_BYTES });
    const unified2 = unifiedR2.ok ? unifiedR2.stdout.toString("utf-8").slice(0, VCS_DIFF_UNIFIED_MAX_BYTES) : "";
    return sendFsResult(queryId, {
      ok: true,
      vcs: "git",
      file,
      sides: {
        before: beforeBuf2.toString("utf-8"),
        after: afterBuf2.toString("utf-8"),
        before_bytes: beforeBuf2.length,
        after_bytes: afterBuf2.length
      },
      unified: unified2
    });
  }
  const afterR = await runVcs(workspaceDir, "hg", ["cat", "-r", rev, file], { maxBuffer: VCS_DIFF_SIDE_MAX_BYTES * 2 });
  if (!afterR.ok) return sendFsResult(queryId, afterR);
  const beforeR = await runVcs(workspaceDir, "hg", ["cat", "-r", `${rev}^`, file], { maxBuffer: VCS_DIFF_SIDE_MAX_BYTES * 2 });
  const beforeBuf = beforeR.ok ? Buffer.from(beforeR.stdout) : Buffer.alloc(0);
  const afterBuf = Buffer.from(afterR.stdout);
  if (isLikelyBinary(beforeBuf) || isLikelyBinary(afterBuf)) {
    return sendFsResult(queryId, { ok: false, code: "binary", error: "binary file" });
  }
  if (beforeBuf.length > VCS_DIFF_SIDE_MAX_BYTES || afterBuf.length > VCS_DIFF_SIDE_MAX_BYTES) {
    return sendFsResult(queryId, { ok: false, code: "too_large", error: "file exceeds 1 MB cap" });
  }
  const unifiedR = await runVcs(workspaceDir, "hg", ["diff", "-c", rev, file], { maxBuffer: VCS_DIFF_UNIFIED_MAX_BYTES });
  const unified = unifiedR.ok ? unifiedR.stdout.toString("utf-8").slice(0, VCS_DIFF_UNIFIED_MAX_BYTES) : "";
  return sendFsResult(queryId, {
    ok: true,
    vcs: "hg",
    file,
    sides: {
      before: beforeBuf.toString("utf-8"),
      after: afterBuf.toString("utf-8"),
      before_bytes: beforeBuf.length,
      after_bytes: afterBuf.length
    },
    unified
  });
}
async function handleVcsOp(op, queryId, params) {
  if (op === "vcs.status") return handleVcsStatus(queryId, params);
  if (op === "vcs.branches") return handleVcsBranches(queryId, params);
  if (op === "vcs.log") return handleVcsLog(queryId, params);
  if (op === "vcs.commit") return handleVcsCommit(queryId, params);
  if (op === "vcs.diff") return handleVcsDiff(queryId, params);
  sendFsResult(queryId, { ok: false, code: "unknown_op", error: `unknown vcs op: ${op}` });
}
async function validateWorkspace(workspaceDir) {
  if (!workspaceDir) return { ok: true, cwd: os.homedir() };
  try {
    const st = await fsp.stat(workspaceDir);
    if (!st.isDirectory()) return { ok: false, error: `workspace_dir "${workspaceDir}" is not a directory` };
    return { ok: true, cwd: workspaceDir };
  } catch {
    return { ok: false, error: `workspace_dir "${workspaceDir}" does not exist on this worker` };
  }
}
async function runWorkerSession(args, msg) {
  const runtimeId = msg && msg.runtime;
  if (!runtimeId) {
    return { sessionId: args.sessionId || null, error: "dispatch did not name a runtime" };
  }
  let runtime = extensionLoader.runtimeFor(runtimeId);
  if (!runtime) {
    try {
      const fetchAsset = cryptoSession ? wsFetchAsset : null;
      await extensionLoader.loadFromHost({ baseUrl: `http://${SERVER_HOST}/worker`, machine: MACHINE_UUID, send, fetchAsset });
      runtimesLoaded = extensionLoader.runtimes.size > 0;
    } catch (err) {
      console.error(`[daemon] on-demand runtime load failed: ${err && err.message || err}`);
    }
    runtime = extensionLoader.runtimeFor(runtimeId);
  }
  if (!runtime) {
    return { sessionId: args.sessionId || null, error: `runtime "${runtimeId}" is not loaded on this worker` };
  }
  let mcpUrl = `http://${SERVER_HOST}/worker/mcp`;
  if (cryptoSession) {
    try {
      mcpUrl = await ensureMcpBridge();
    } catch (err) {
      console.error(`[daemon] MCP bridge unavailable, falling back to direct URL: ${err && err.message || err}`);
    }
  }
  const responseChunks = [];
  let announced = null;
  const emit = (te) => {
    if (!te || typeof te.type !== "string") return;
    const psid = te.sessionId;
    if (psid && psid !== announced) {
      announced = psid;
      send({ type: "execution.event", executionId: msg.executionId, event: { type: "session_id", sessionId: psid } });
    }
    send({ type: "execution.event", executionId: msg.executionId, event: te });
    if (te.type === "text" && te.partial === false && typeof te.text === "string") responseChunks.push(te.text);
  };
  const input = {
    sessionId: args.sessionId || "",
    systemPrompt: args.systemPrompt,
    userPrompt: args.userPrompt,
    resume: !!args.sessionId,
    workspaceDir: args.cwd,
    // Per-session provider model + reasoning depth from the dispatch — the
    // runtime applies them over its default when present.
    model: msg && msg.model || void 0,
    reasoningEffort: msg && msg.reasoningEffort || void 0,
    persistent: !!(msg && msg.persistent),
    // The session persona's preamble text — the runtime prepends it to the
    // system prompt. Absent when the session has no persona.
    personaPrompt: msg && msg.personaPrompt || void 0,
    mcpEndpoint: { url: mcpUrl, auth: { "X-Frontier-Execution-Id": msg.executionId } },
    userMcpServers: args.mcpServers || void 0,
    env: args.extraEnv || void 0,
    instructions: msg.instructions || void 0,
    executionId: msg.executionId,
    role: args.role || msg.role || "",
    emit,
    signal: args.abortController ? args.abortController.signal : void 0
  };
  try {
    const res = await runtime.run(input);
    const responseText = res && res.responseText != null ? res.responseText : responseChunks.length ? responseChunks.join("") : null;
    return {
      sessionId: res && res.providerSessionId || args.sessionId || null,
      error: res && res.error || null,
      responseText,
      usage: res && res.usage || null
    };
  } catch (err) {
    return { sessionId: args.sessionId || null, error: `runtime exception: ${err && err.message ? err.message : String(err)}` };
  }
}
async function runExecute(msg) {
  const { executionId, role } = msg;
  let result;
  try {
    if (role === "init") result = await handleInit(msg);
    else if (role === "handshake") result = await handleHandshake(msg);
    else if (role === "extension") result = await handleExtensionTurn(msg);
    else if (role === "child_report") result = await handleChildReportTurn(msg);
    else result = { outcome: "error", error: `unknown role: ${role}` };
  } finally {
    inflight.delete(executionId);
    sendHeartbeat();
  }
  let outcome = result.outcome;
  if (outcome === "error" && result.error === "cancelled") outcome = "cancelled";
  send({
    type: "execution.result",
    machine: MACHINE_UUID,
    reservationId: msg.reservationId || null,
    executionId,
    sessionId: result.sessionId || null,
    outcome,
    persist: result.persist || null,
    error: outcome === "cancelled" ? void 0 : result.error || void 0,
    responseText: result.responseText || void 0,
    // Additive: the turn's token usage (camelCase TokenUsage shape) when
    // the adapter reported it; absent for the anthropic_cli SDK path.
    usage: result.usage || void 0
  });
  console.log(`[daemon] done ${executionId} (${role}) \u2014 ${outcome}${result.error && outcome !== "cancelled" ? " / " + result.error : ""}`);
}
async function handleHandshake(msg) {
  const ws_check = await validateWorkspace(msg.workspaceDir);
  if (!ws_check.ok) return { outcome: "error", error: ws_check.error };
  const ac = new AbortController();
  trackInflight(msg.executionId, ac, inflightMetaFromMsg(msg));
  const { sessionId, error } = await runWorkerSession({
    systemPrompt: msg.systemPrompt,
    userPrompt: msg.userPrompt,
    sessionId: msg.sessionId,
    cwd: ws_check.cwd,
    executionId: msg.executionId,
    role: "handshake",
    abortController: ac,
    mcpServers: msg.mcpServers
  }, msg);
  return error ? { outcome: "error", sessionId, error } : { outcome: "success", sessionId };
}
async function handleInit(msg) {
  const ws_check = await validateWorkspace(msg.workspaceDir);
  if (!ws_check.ok) return { outcome: "error", error: ws_check.error };
  const ac = new AbortController();
  trackInflight(msg.executionId, ac, inflightMetaFromMsg(msg));
  const { sessionId, error } = await runWorkerSession({
    systemPrompt: msg.systemPrompt,
    userPrompt: msg.userPrompt || "ready",
    sessionId: null,
    cwd: ws_check.cwd,
    executionId: msg.executionId,
    role: "init",
    abortController: ac,
    mcpServers: msg.mcpServers
  }, msg);
  return error ? { outcome: "error", sessionId, error } : { outcome: "success", sessionId };
}
async function handleExtensionTurn(msg) {
  const ws_check = await validateWorkspace(msg.workspaceDir);
  if (!ws_check.ok) return { outcome: "error", error: ws_check.error };
  const persistKey = msg.persistent && msg.metadata && msg.metadata.sessionId ? `session-${msg.metadata.sessionId}` : msg.executionId;
  const persistDir = path.join("/tmp", "frontier", persistKey);
  await fsp.mkdir(persistDir, { recursive: true });
  const persistPath = path.join(persistDir, "_persist.json");
  const ac = new AbortController();
  trackInflight(msg.executionId, ac, inflightMetaFromMsg(msg));
  const { sessionId, error, responseText, usage } = await runWorkerSession({
    systemPrompt: msg.systemPrompt,
    userPrompt: msg.userPrompt,
    sessionId: msg.sessionId,
    cwd: ws_check.cwd,
    executionId: msg.executionId,
    role: "extension",
    abortController: ac,
    extraEnv: { FRONTIER_PERSIST_DIR: persistDir }
  }, msg);
  let persistPayload = null;
  try {
    const raw = await fsp.readFile(persistPath, "utf-8");
    persistPayload = JSON.parse(raw);
  } catch {
  }
  try {
    await fsp.rm(persistDir, { recursive: true, force: true });
  } catch {
  }
  return error ? { outcome: "error", sessionId, error, persist: persistPayload, usage } : { outcome: "success", sessionId, persist: persistPayload, responseText, usage };
}
async function handleChildReportTurn(msg) {
  const ws_check = await validateWorkspace(msg.workspaceDir);
  if (!ws_check.ok) return { outcome: "error", error: ws_check.error };
  const persistDir = path.join("/tmp", "frontier", msg.executionId);
  await fsp.mkdir(persistDir, { recursive: true });
  const persistPath = path.join(persistDir, "_persist.json");
  const ac = new AbortController();
  trackInflight(msg.executionId, ac, inflightMetaFromMsg(msg));
  const { sessionId, error, usage } = await runWorkerSession({
    systemPrompt: msg.systemPrompt,
    userPrompt: msg.userPrompt,
    sessionId: msg.sessionId,
    cwd: ws_check.cwd,
    executionId: msg.executionId,
    role: "child_report",
    abortController: ac,
    extraEnv: { FRONTIER_PERSIST_DIR: persistDir }
  }, msg);
  let persistPayload = null;
  try {
    const raw = await fsp.readFile(persistPath, "utf-8");
    persistPayload = JSON.parse(raw);
  } catch {
  }
  try {
    await fsp.rm(persistDir, { recursive: true, force: true });
  } catch {
  }
  return error ? { outcome: "error", sessionId, error, persist: persistPayload, usage } : { outcome: "success", sessionId, persist: persistPayload, usage };
}
process.on("uncaughtException", (err) => {
  console.error("[daemon] FATAL uncaughtException:", err && err.stack || err);
  process.exit(1);
});
process.on("unhandledRejection", (err) => {
  console.error("[daemon] FATAL unhandledRejection:", err && err.stack || err);
  process.exit(1);
});
var IDENTITY_DIR = path.join(os.homedir(), ".frontier-worker");
var IDENTITY_FILE = path.join(IDENTITY_DIR, "identity.json");
function readIdentity() {
  try {
    const raw = fs.readFileSync(IDENTITY_FILE, "utf-8");
    const id = JSON.parse(raw);
    if (id && typeof id.machineId === "string" && typeof id.token === "string") return id;
  } catch {
  }
  return null;
}
function writeIdentity(identity) {
  fs.mkdirSync(IDENTITY_DIR, { recursive: true });
  fs.writeFileSync(IDENTITY_FILE, JSON.stringify(identity, null, 2), { mode: 384 });
}
function pairOverSocket(sock, code) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let session = null;
    const eph = workerCrypto.generateEphemeralKeyPair();
    const done = (err, identity) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        sock.close();
      } catch {
      }
      err ? reject(err) : resolve(identity);
    };
    const timer = setTimeout(() => done(new Error("pairing timed out")), 3e4);
    const sendHello = () => {
      sock.send(JSON.stringify({
        type: "crypto_hello",
        mode: "pair",
        pub: workerCrypto.exportPublicKeyRaw(eph.publicKey).toString("base64")
      }));
    };
    if (sock.readyState === WebSocket.OPEN) sendHello();
    else sock.on("open", sendHello);
    sock.on("message", (data) => {
      if (!session) {
        let hello;
        try {
          hello = JSON.parse(data.toString());
        } catch {
          return;
        }
        if (hello.type !== "crypto_hello") {
          if (hello.type === "pair_error") done(new Error(`host rejected pairing: ${hello.error || "unknown"}`));
          return;
        }
        try {
          const hostEphPub = Buffer.from(String(hello.pub || ""), "base64");
          const ephSecret = workerCrypto.computeSharedSecret(eph.privateKey, hostEphPub);
          session = workerCrypto.createSession(ephSecret, code, "worker");
          session._hostStaticPub = typeof hello.spub === "string" ? hello.spub : null;
          sock.send(session.sealText({ type: "pair", hostname: os.hostname() }));
        } catch (err) {
          done(new Error(`pairing handshake failed: ${err && err.message || err}`));
        }
        return;
      }
      let msg;
      try {
        msg = session.openText(frameToBuf(data));
      } catch {
        return done(new Error("could not decrypt host reply \u2014 wrong code or MITM"));
      }
      if (msg.type === "paired" && msg.machineId && msg.token) {
        const identity = {
          machineId: msg.machineId,
          token: msg.token,
          name: msg.name || os.hostname(),
          // The directly-dialable address we were given, if any. Null for a bare
          // `connect <code>` — that worker only ever reaches the host via Link,
          // so there's no direct address to remember.
          host: EXPLICIT_HOST || null,
          reachId: msg.reachId || null,
          // Prefer the static key from the encrypted reply; fall back to the
          // one from the plaintext hello (same value, public either way).
          hostStaticPub: msg.hostStaticPub || session._hostStaticPub || null
        };
        try {
          writeIdentity(identity);
        } catch (err) {
          return done(new Error(`could not persist identity: ${err && err.message || err}`));
        }
        console.log(`[daemon] paired as ${identity.machineId} ("${identity.name}") \u2014 identity saved to ${IDENTITY_FILE}`);
        done(null, identity);
      } else if (msg.type === "pair_error") {
        done(new Error(`host rejected pairing: ${msg.error || "unknown"}`));
      }
    });
    sock.on("error", (err) => done(new Error(`pairing socket error: ${err && err.message || err}`)));
    sock.on("close", () => done(new Error("pairing socket closed before completion")));
  });
}
async function resolveConnectIdentity(code) {
  const existing = readIdentity();
  if (existing) {
    console.log(`[daemon] using stored identity ${existing.machineId} ("${existing.name}")`);
    return existing;
  }
  if (!code) throw new Error("no stored identity and no pairing code given \u2014 usage: connect [host:port] <code>");
  let delay = 2e3;
  for (; ; ) {
    try {
      const sock = await dialForPairing(code);
      return await pairOverSocket(sock, code);
    } catch (err) {
      console.error(`[daemon] ${err && err.message || err}; retrying in ${delay}ms`);
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 3e4);
    }
  }
}
function applyExplicitHost(addr) {
  EXPLICIT_HOST = addr;
  SERVER_HOST = addr;
}
async function bootstrap() {
  const argv = process.argv.slice(2);
  if (argv[0] === "connect") {
    const rest = argv.slice(1);
    let code = null;
    if (rest.length >= 2) {
      applyExplicitHost(rest[0]);
      code = rest[1];
    } else if (rest.length === 1) {
      code = rest[0];
      if (process.env.FRONTIER_SERVER_HOST) applyExplicitHost(process.env.FRONTIER_SERVER_HOST);
    }
    PAIRED = true;
    let identity = await resolveConnectIdentity(code);
    if (!identity.hostStaticPub) {
      console.error("[daemon] stored identity predates crypto (no host static key) \u2014 re-pairing");
      try {
        fs.unlinkSync(IDENTITY_FILE);
      } catch {
      }
      identity = await resolveConnectIdentity(code);
    }
    MACHINE_UUID = identity.machineId;
    MACHINE_TOKEN = identity.token;
    HOST_STATIC_PUB = identity.hostStaticPub || null;
    DAEMON_REACH_ID = identity.reachId || null;
    if (identity.host) applyExplicitHost(identity.host);
  } else if (MACHINE_UUID) {
    applyExplicitHost(SERVER_HOST);
  } else {
    console.error("FRONTIER_MACHINE_UUID is required (or run with: connect [host:port] <code>)");
    process.exit(1);
  }
  claimPidLock();
  connect();
}
bootstrap().catch((err) => {
  console.error("[daemon] FATAL bootstrap:", err && err.stack || err);
  process.exit(1);
});
