#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>

using namespace std;

class Read_file {

    //myfile.open("log_example.txt",ios::in);
};

int main () {
    string line;
    ifstream myfile;
    myfile.open("../log_input/log_example.txt");
    if (myfile.is_open())
{
    while ( getline (myfile,line) )
    {
        string parsed, input=line;
        stringstream input_stringstream(input);
        //cout << line << '\n';
        if(getline(input_stringstream,parsed,' '))
{
     cout << parsed <<'\n';
}
        
        

    
       //cout << line << '\n';
    } 
    myfile.close();
}    

  else cout << "Unable to open file"; 
  
    return 0;

}
