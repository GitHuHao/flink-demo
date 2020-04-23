package com.bigdata.flink.item.transform.uv

class Bloom(size:Long) extends Serializable {

  private val cap = size

  def hash(value:String,seed:Int):Long = {
    var result = 0
    for(char <- value){
      result = result * seed + char
    }
    (cap - 1) & result // 裁剪超出阈值的数
  }
}
