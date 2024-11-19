using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

class Program
{

    const int SERIES_SIZE_10MB = 10 * 1024 * 1024;
    const int SERIES_SIZE_100MB = 100 * 1024 * 1024;
    const int SERIES_SIZE_500MB = 500 * 1024 * 1024; 

    static async Task Main(string[] args)
    {

        GenerateTestFile("input.txt", 175000000);
        Stopwatch sw = new Stopwatch();

    
        string seriesSortedFile = ModifySeriesSort("input.txt", SERIES_SIZE_500MB);


        sw.Start();
        await ExternalMergeSort(seriesSortedFile, "output.txt");
        File.Delete(seriesSortedFile);  
        sw.Stop();


        Console.WriteLine($"{sw.Elapsed.Minutes} хвилин, {sw.Elapsed.Seconds} секунд, {sw.Elapsed.Milliseconds} мілісекунд.");
    }

    private static void GenerateTestFile(string fileName, int numElements)
    {
        Random rand = new Random();
        using (StreamWriter sw = new StreamWriter(fileName))
        {
            for (int i = 0; i < numElements; i++)
            {
                int value = rand.Next(0, 10001);
                sw.WriteLine(value);
            }
        }
    }

    
    private static string ModifySeriesSort(string inputFilePath, int bufferSize)
    {
        string tempFile = "TempInput.txt";
        List<int> currentBuffer = new List<int>();
        int currentBufferSize = 0;

        using (StreamReader reader = new StreamReader(inputFilePath))
        using (StreamWriter writer = new StreamWriter(tempFile))
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                int value = int.Parse(line);
                currentBuffer.Add(value);
                currentBufferSize += sizeof(int);

                if (currentBufferSize > bufferSize)
                {
                    currentBuffer.Sort();
                    foreach (var num in currentBuffer)
                    {
                        writer.WriteLine(num);
                    }

                    currentBuffer.Clear();
                    currentBufferSize = 0;
                }
            }

            if (currentBuffer.Count > 0)
            {
                currentBuffer.Sort();
                foreach (var num in currentBuffer)
                {
                    writer.WriteLine(num);
                }
            }
        }

        return tempFile;
    }

    
    private static async Task ExternalMergeSort(string inputFilePath, string outputFilePath)
    {
        string bufferA = "bufferA.txt";
        string bufferB = "bufferB.txt";

        
        await NaturalSplitToFile(inputFilePath, bufferA, bufferB);


        while (!IsFileEmpty(bufferA) && !IsFileEmpty(bufferB))
        {
            string tempOutput = "tempOutput.txt";
            await MergeBuffersParallel(bufferA, bufferB, tempOutput);
            File.Copy(tempOutput, outputFilePath, true);
            File.Delete(tempOutput);
        }
    }

    
    private static bool IsFileEmpty(string filePath)
    {
        return new FileInfo(filePath).Length == 0;
    }


    private static async Task NaturalSplitToFile(string inputFilePath, string outputFilePath1, string outputFilePath2)
    {
        using (StreamReader reader = new StreamReader(inputFilePath))
        {
            using (StreamWriter writer1 = new StreamWriter(outputFilePath1))
            using (StreamWriter writer2 = new StreamWriter(outputFilePath2))
            {
                string line;
                bool switchWriter = true;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (switchWriter)
                    {
                        await writer1.WriteLineAsync(line);
                    }
                    else
                    {
                        await writer2.WriteLineAsync(line);
                    }
                    switchWriter = !switchWriter;
                }
            }
        }
    }

    
    private static async Task MergeBuffersParallel(string inputFilePath1, string inputFilePath2, string outputFilePath)
    {
        using (StreamReader reader1 = new StreamReader(inputFilePath1))
        using (StreamReader reader2 = new StreamReader(inputFilePath2))
        using (StreamWriter writer = new StreamWriter(outputFilePath))
        {
            string line1 = await reader1.ReadLineAsync();
            string line2 = await reader2.ReadLineAsync();

            var task1 = reader1.ReadLineAsync();
            var task2 = reader2.ReadLineAsync();

            while (line1 != null || line2 != null)
            {
                int num1 = (line1 != null) ? int.Parse(line1) : int.MaxValue;
                int num2 = (line2 != null) ? int.Parse(line2) : int.MaxValue;

                if (num1 <= num2)
                {
                    await writer.WriteLineAsync(num1.ToString());
                    line1 = await task1;
                    task1 = reader1.ReadLineAsync();
                }
                else
                {
                    await writer.WriteLineAsync(num2.ToString());
                    line2 = await task2;
                    task2 = reader2.ReadLineAsync();
                }
            }
        }
    }
}
