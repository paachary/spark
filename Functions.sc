// Author - Prashant Acharya
// Sample code for illustrating named and anonymous (a.k.a. lambda functions) functions.


def findAscii(char :Char ) : Int = {
  char.toInt
}

println(findAscii('a'))

/*
-------------------------------------------------------------------
*/

def findAsciiLambda(char :Char, f : Char => Int) : Int = {
  f(char)
}

findAsciiLambda('f', findAscii)

findAsciiLambda('d', char => {
  val x : Int = char.toInt; x
})

/*
-------------------------------------------------------------------
*/

def convertToUpper(string: String) : String = {
  var convertedString: String = ""
  for ( i <- 0 to string.length-1) {

    //97 and 122

    if ( string(i).toInt >= 97 && string(i).toInt <= 122) {
      convertedString = convertedString+""+(string(i).toInt-32).toChar
    } else {
      convertedString = convertedString + "" + string(i)
    }
  }
  convertedString
}

/*
-------------------------------------------------------------------
*/

def convertToLower(string: String) : String = {
  var convertedString: String = ""
  for ( i <- 0 to string.length-1) {

    //65 and 90

    if ( string(i).toInt >= 65 && string(i).toInt <= 90) {
      convertedString = convertedString+""+(string(i).toInt+32).toChar
    } else {
      convertedString = convertedString + "" + string(i)
    }
  }
  convertedString
}

/*
-------------------------------------------------------------------
*/

def transformCase( string : String, f : String => String ) : String = {
  f(string)
}

var string : String = "I am Prashant"
transformCase(string, convertToLower)
transformCase(string, convertToUpper)

transformCase("I am a very good engineer" ,
  convertedString  => {
    var newString : String = ""
    for ( i <- 0 to convertedString.length-1) {
      //97 and 122
      if ( convertedString(i).toInt >= 97 && convertedString(i).toInt <= 122) {
        newString = newString+""+(convertedString(i).toInt-32).toChar
      } else {
        newString = newString + "" + convertedString(i)
      }
    }
    newString
  })

/*
-------------------------------------------------------------------
*/

// Fibonacci Series

var prev_1 = 0
var sum = 0
var prev_2 = 1
println(prev_1)
println(prev_2)
for (i <- 1 to 9) {
  sum = prev_1 + prev_2
  println(sum)
  prev_1 = prev_2
  prev_2 = sum
}
/*
-------------------------------------------------------------------
*/


