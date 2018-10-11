package ScalaWriter

import java.io.BufferedWriter
import java.io.FileWriter

object Writer{
  def Write(text: String, fileName :String ){
    val fw: FileWriter = new FileWriter (fileName, true)
    val bw: BufferedWriter = new BufferedWriter (fw)
    bw.write (text)
    bw.newLine ()
    bw.close ()
  }
}