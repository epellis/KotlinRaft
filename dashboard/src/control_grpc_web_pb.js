/**
 * @fileoverview gRPC-Web generated client stub for kotlinraft
 * @enhanceable
 * @public
 */

// GENERATED CODE -- DO NOT EDIT!



const grpc = {};
grpc.web = require('grpc-web');

const proto = {};
proto.kotlinraft = require('./control_pb.js');

/**
 * @param {string} hostname
 * @param {?Object} credentials
 * @param {?Object} options
 * @constructor
 * @struct
 * @final
 */
proto.kotlinraft.ControlClient =
    function(hostname, credentials, options) {
  if (!options) options = {};
  options['format'] = 'text';

  /**
   * @private @const {!grpc.web.GrpcWebClientBase} The client
   */
  this.client_ = new grpc.web.GrpcWebClientBase(options);

  /**
   * @private @const {string} The hostname
   */
  this.hostname_ = hostname;

};


/**
 * @param {string} hostname
 * @param {?Object} credentials
 * @param {?Object} options
 * @constructor
 * @struct
 * @final
 */
proto.kotlinraft.ControlPromiseClient =
    function(hostname, credentials, options) {
  if (!options) options = {};
  options['format'] = 'text';

  /**
   * @private @const {!grpc.web.GrpcWebClientBase} The client
   */
  this.client_ = new grpc.web.GrpcWebClientBase(options);

  /**
   * @private @const {string} The hostname
   */
  this.hostname_ = hostname;

};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kotlinraft.Entry,
 *   !proto.kotlinraft.SetStatus>}
 */
const methodDescriptor_Control_SetEntry = new grpc.web.MethodDescriptor(
  '/kotlinraft.Control/SetEntry',
  grpc.web.MethodType.UNARY,
  proto.kotlinraft.Entry,
  proto.kotlinraft.SetStatus,
  /**
   * @param {!proto.kotlinraft.Entry} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kotlinraft.SetStatus.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kotlinraft.Entry,
 *   !proto.kotlinraft.SetStatus>}
 */
const methodInfo_Control_SetEntry = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kotlinraft.SetStatus,
  /**
   * @param {!proto.kotlinraft.Entry} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kotlinraft.SetStatus.deserializeBinary
);


/**
 * @param {!proto.kotlinraft.Entry} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kotlinraft.SetStatus)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kotlinraft.SetStatus>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kotlinraft.ControlClient.prototype.setEntry =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kotlinraft.Control/SetEntry',
      request,
      metadata || {},
      methodDescriptor_Control_SetEntry,
      callback);
};


/**
 * @param {!proto.kotlinraft.Entry} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kotlinraft.SetStatus>}
 *     A native promise that resolves to the response
 */
proto.kotlinraft.ControlPromiseClient.prototype.setEntry =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kotlinraft.Control/SetEntry',
      request,
      metadata || {},
      methodDescriptor_Control_SetEntry);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kotlinraft.Key,
 *   !proto.kotlinraft.RemoveStatus>}
 */
const methodDescriptor_Control_RemoveEntry = new grpc.web.MethodDescriptor(
  '/kotlinraft.Control/RemoveEntry',
  grpc.web.MethodType.UNARY,
  proto.kotlinraft.Key,
  proto.kotlinraft.RemoveStatus,
  /**
   * @param {!proto.kotlinraft.Key} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kotlinraft.RemoveStatus.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kotlinraft.Key,
 *   !proto.kotlinraft.RemoveStatus>}
 */
const methodInfo_Control_RemoveEntry = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kotlinraft.RemoveStatus,
  /**
   * @param {!proto.kotlinraft.Key} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kotlinraft.RemoveStatus.deserializeBinary
);


/**
 * @param {!proto.kotlinraft.Key} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kotlinraft.RemoveStatus)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kotlinraft.RemoveStatus>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kotlinraft.ControlClient.prototype.removeEntry =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kotlinraft.Control/RemoveEntry',
      request,
      metadata || {},
      methodDescriptor_Control_RemoveEntry,
      callback);
};


/**
 * @param {!proto.kotlinraft.Key} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kotlinraft.RemoveStatus>}
 *     A native promise that resolves to the response
 */
proto.kotlinraft.ControlPromiseClient.prototype.removeEntry =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kotlinraft.Control/RemoveEntry',
      request,
      metadata || {},
      methodDescriptor_Control_RemoveEntry);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kotlinraft.Key,
 *   !proto.kotlinraft.GetStatus>}
 */
const methodDescriptor_Control_GetEntry = new grpc.web.MethodDescriptor(
  '/kotlinraft.Control/GetEntry',
  grpc.web.MethodType.UNARY,
  proto.kotlinraft.Key,
  proto.kotlinraft.GetStatus,
  /**
   * @param {!proto.kotlinraft.Key} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kotlinraft.GetStatus.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kotlinraft.Key,
 *   !proto.kotlinraft.GetStatus>}
 */
const methodInfo_Control_GetEntry = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kotlinraft.GetStatus,
  /**
   * @param {!proto.kotlinraft.Key} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kotlinraft.GetStatus.deserializeBinary
);


/**
 * @param {!proto.kotlinraft.Key} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kotlinraft.GetStatus)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kotlinraft.GetStatus>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kotlinraft.ControlClient.prototype.getEntry =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kotlinraft.Control/GetEntry',
      request,
      metadata || {},
      methodDescriptor_Control_GetEntry,
      callback);
};


/**
 * @param {!proto.kotlinraft.Key} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kotlinraft.GetStatus>}
 *     A native promise that resolves to the response
 */
proto.kotlinraft.ControlPromiseClient.prototype.getEntry =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kotlinraft.Control/GetEntry',
      request,
      metadata || {},
      methodDescriptor_Control_GetEntry);
};


module.exports = proto.kotlinraft;

