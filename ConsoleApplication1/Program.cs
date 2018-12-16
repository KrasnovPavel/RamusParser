using System;

namespace RamusParser
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Enter input file name and output filename: ramusparser.exe file1.rsf file2.csv");
                return;
            }
            RamusParser.ReadRamusFile(args[0], args[1]);
        }
    }
}