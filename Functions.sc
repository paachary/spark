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

var string : String = "I am Prashant"
println(convertToUpper(string))


/*
-------------------------------------------------------------------
*/

def transformCase( string : String, f : String => String ) : String = {
  f(string)
}

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

