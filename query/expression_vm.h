//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef QUERY_EXPRESSION_VM_H_
#define QUERY_EXPRESSION_VM_H_
#include <stddef.h>
#include <stdint.h>

// The expression VM is a stack based interpreter that evaluates expressions
// (filter, dimension, pre-aggregate measure) on GPUs.
// For simplicity each instruction is a 32-bit word using the following format:
//   Bits masked by 0xff000000 defines the instruction itself.
//   Bits masked by 0x0000ffff defines the payload of the instruction.
//
// A pair of value/null stack is used to process SQL expressions.
// Each slot in the value stack always takes 4 bytes, regardless of the type,
// while each slot in the null stack takes 1 byte.
// Both stack grows up from lower address to higher address.
enum ExprVMInst {
  // End of the instruction sequence, stops the expression VM.
  INST_EOF               = 0,

  // Invokes a function call with the top X parameters on the stack pair.
  // The last parameter is on top of the stack, while the first is the deepest
  // in the stack.
  // The call type is specified in the payload (masked by 0x0000ffff).
  // At the end of the call, the result is put on top of the stack pair.
  //
  // TODO(lucafuji): to support an argument with variable-length data
  // (e.g., geofence),
  // We need to add a third stack for storing offsets to the value stack,
  // and introduce new instructions that operate on the offset stack.
  INST_CALL              = 0x01000000,

  // Reads from an input column and pushes it onto the top of the stack pair.
  // The payload (masked by 0x0000ffff) defines index to the column to be read
  // from the global (query-wise) column array. Values are always stored using
  // 4 bytes regardless of their types for simplicity.
  INST_PUSH              = 0x02000000,

  // The following instructions push a constant onto the top of the stack pair.
  // To push a small integer that fits in a 16-bit word, simply put the integer
  // as the payload of INST_PUSH_CONST_LOW.
  // To push a big integer that does not fit, first split it to lower 16 bits
  // and higher 16 bits, then put the lower 16 bits as the payload of
  // INST_PUSH_CONST_LOW, then put the higher 16 bits as the payload of
  // INST_WRITE_CONST_HIGH. Note that INST_WRITE_CONST_HIGH writes the payload
  // to the higher 16 bits of the word on top of the value stack, without moving
  // the stack pointer.
  INST_PUSH_CONST_NULL   = 0x03000000,
  INST_PUSH_CONST_LOW    = 0x04000000,
  INST_WRITE_CONST_HIGH  = 0x05000000,

  // Skips the next X instructions when the top of the value stack is
  // zero/non-zero. X is specified in the payload (masked by 0x0000ffff).
  // The purpose of this instruction is to allow short-circuiting in logical
  // expresions, and to allow case-when-then-else-end (if-else) expression.
  INST_JZ                = 0x06000000,
  INST_JNZ               = 0x07000000,

  // Skips the next X instructions.
  // X is specified in the payload (masked by 0x0000ffff).
  // The purpose of this instruction is to allow case-when-then-else-end
  // (if-else) expression (jumping from the end of if block to end of else).
  INST_JMP               = 0x08000000
};

// ExprVMCall defines the call types of all operators and (non-aggregate)
// functions. UDF functions also share the same call type space.
enum ExprVMCall {
  // Unary OPs
  CALL_NEG               = 0x01,
  CALL_NEG_FLOAT         = 0x02,
  CALL_NOT               = 0x03,
  CALL_BITWISE_NOT       = 0x04,
  CALL_SIGNED_TO_FLOAT   = 0x05,
  CALL_UNSIGNED_TO_FLOAT = 0x06,
  CALL_FLOAT_TO_SIGNED   = 0x07,
  CALL_FLOAT_TO_UNSIGNED = 0x08,

  // Operators above this line do not need to modify the null stack.
  call_begin_null_ops    = 0x0d,

  CALL_IS_NULL           = 0x0e,
  CALL_IS_NOT_NULL       = 0x0f,

  // Binary OPs
  call_begin_binary_ops  = 0x10,

  CALL_IS                = 0x11,
  // In SQL expression: NULL AND FALSE = FALSE, NULL OR TRUE = TRUE...
  CALL_AND               = 0x12,
  CALL_OR                = 0x13,

  call_end_null_ops      = 0x14,
  // Operators below this line can simply run nulls[0] = nulls[0] && nulls[1] to
  // handle nulls. Note that the null stack stores validity.

  CALL_XOR               = 0x21,
  CALL_BITWISE_AND       = 0x22,
  CALL_BITWISE_OR        = 0x23,
  CALL_BITWISE_XOR       = 0x24,
  CALL_SHL               = 0x25,
  CALL_SHR               = 0x26,
  CALL_EQ                = 0x27,
  CALL_EQ_FLOAT          = 0x28,
  CALL_LT_SIGNED         = 0x29,
  CALL_LT_UNSIGNED       = 0x2a,
  CALL_LT_FLOAT          = 0x2b,
  CALL_LE_SIGNED         = 0x2c,
  CALL_LE_UNSIGNED       = 0x2d,
  CALL_LE_FLOAT          = 0x2e,

  CALL_ADD               = 0x31,
  CALL_ADD_FLOAT         = 0x32,
  CALL_SUB               = 0x33,
  CALL_SUB_FLOAT         = 0x34,
  CALL_MUL_SIGNED        = 0x35,
  CALL_MUL_UNSIGNED      = 0x36,
  CALL_MUL_FLOAT         = 0x37,
  CALL_DIV_SIGNED        = 0x38,
  CALL_DIV_UNSIGNED      = 0x39,
  CALL_DIV_FLOAT         = 0x3a,
  CALL_MOD_SIGNED        = 0x3b,
  CALL_MOD_UNSIGNED      = 0x3c,
  CALL_MOD_FLOAT         = 0x3d
};
#endif  // QUERY_EXPRESSION_VM_H_
