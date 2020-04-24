package org.apache.spark.sql.catalyst.expressions.aggregate

import java.io.FileInputStream
import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, TypeUtils}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes}

case class WasmAggregation (
  child: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[ByteBuffer] {

  def this(child: Expression) = this(child, 0 ,0)


  override lazy val deterministic: Boolean = false

  private lazy val projection = UnsafeProjection.create(
    Array[DataType](ArrayType(elementType = child.dataType, containsNull = false)))
  private  lazy val row = new UnsafeRow(1)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "wasm function")


  /**
   * Creates an empty aggregation buffer object. This is called before processing each key group
   * (group by key).
   *
   * @return an aggregation buffer object
   */
  override def createAggregationBuffer(): ByteBuffer = ByteBuffer.allocateDirect(1024*5)


  System.loadLibrary("wasm_bootstrap")

  /**
   * @arg program a directly mapped Byte buffer containing the wasm program to execute
   * @arg inputData a directly mapped Byte buffer containing the inputData to the program
   **/
  @native private def reduce(
    program: ByteBuffer, programLength: Int,
    inputData: ByteBuffer, inputLength: Int): Int


  @throws[Exception]
  def evalFromBuffer(input: ByteBuffer): Long = {
    val buf = ByteBuffer.allocateDirect(80000)
    val in = new FileInputStream("foo.wasm")
    val len = in.getChannel.read(buf)

    val inputBuffer = input.duplicate().flip()

    reduce(buf, len, inputBuffer, inputBuffer.limit())
  }

  /**
   * Updates the aggregation buffer object with an input row and returns a new buffer object. For
   * performance, the function may do in-place update and return it instead of constructing new
   * buffer object.
   *
   * This is typically called when doing Partial or Complete mode aggregation.
   *
   * @param buffer The aggregation buffer object.
   * @param input  an input row
   */
  override def update(buffer: ByteBuffer, input: InternalRow): ByteBuffer = {
    assert(child.dataType.sameType(DataTypes.LongType))
    val longValue = child.eval(input).asInstanceOf[Long]

    buffer.putLong(longValue)
  }

  /**
   * Merges an input aggregation object into aggregation buffer object and returns a new buffer
   * object. For performance, the function may do in-place merge and return it instead of
   * constructing new buffer object.
   *
   * This is typically called when doing PartialMerge or Final mode aggregation.
   *
   * @param buffer the aggregation buffer object used to store the aggregation result.
   * @param input  an input aggregation object. Input aggregation object can be produced by
   *               de-serializing the partial aggregate's output from Mapper side.
   */
  override def merge(buffer: ByteBuffer, input: ByteBuffer): ByteBuffer = {
    //ToDo: Make WASM

    val valueToadd = evalFromBuffer(input)

    buffer.putLong(valueToadd)
  }

  /**
   * Generates the final aggregation result value for current key group with the aggregation buffer
   * object.
   *
   * Developer note: the only return types accepted by Spark are:
   *   - primitive types
   *   - InternalRow and subclasses
   *   - ArrayData
   *   - MapData
   *
   * @param buffer aggregation buffer object.
   * @return The aggregation result of current key group
   */
  override def eval(buffer: ByteBuffer): Any = evalFromBuffer(buffer)

  /** Serializes the aggregation buffer object T to Array[Byte] */
  override def serialize(buffer: ByteBuffer): Array[Byte] = {
    val bufferRefToSerialize = buffer.duplicate().flip().asLongBuffer()

    val bufferCopy = new Array[Long](bufferRefToSerialize.limit())
    bufferRefToSerialize.get(bufferCopy)
    val array = new GenericArrayData(bufferCopy)
    projection.apply(InternalRow.apply(array)).getBytes()
  }

  /** De-serializes the serialized format Array[Byte], and produces aggregation buffer object T */
  override def deserialize(bytes: Array[Byte]): ByteBuffer = {
    val buffer = createAggregationBuffer()

    row.pointTo(bytes, bytes.length)
    row.getArray(0).foreach(child.dataType, (_, x: Any) => buffer.putLong(x.asInstanceOf[Long]))
    buffer
  }

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = DataTypes.LongType

  override def nullable: Boolean = true

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  override def children: Seq[Expression] = child :: Nil

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}
