#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <future>
#include <string>
#include <cmath>
#include <iterator>
#include <algorithm>
#include <stdio.h>
#include <stack>

class ThreadPool
{
public:
  ThreadPool(int nThreads)
  {
    resize(nThreads);
  }
  void putTask(std::packaged_task<std::vector<char>()> &&task)
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_tasks.push(std::move(task));
    m_mutex_condition.notify_one();
  }

  void stop()
  {
    {
      std::unique_lock<std::mutex> lock(m_mutex);
      m_shouldTerminate = true;
    }
    m_mutex_condition.notify_all();
    for (std::thread &thread : m_threads)
    {
      thread.join();
    }
    m_threads.clear();
  }

  void resize(int nThreads)
  {

    if (nThreads <= 0)
    {
      nThreads = 1;
    }

    if (nThreads < m_threads.size())
    {
      stop();
      m_threads.clear();

      for (uint32_t i = 0; i < nThreads; i++)
      {
        m_threads.emplace_back(&ThreadPool::threadLoop, this);
      }
    }
    else
    {
      m_threads.reserve(nThreads);

      for (uint32_t i = 0; i < nThreads; i++)
      {
        m_threads.emplace_back(&ThreadPool::threadLoop, this);
      }
    }
  }

  int threadActive() const
  {
    return m_threads.size();
  }

private:
  std::vector<std::thread> m_threads;
  std::mutex m_mutex;
  std::condition_variable m_mutex_condition;
  std::queue<std::packaged_task<std::vector<char>()>> m_tasks;
  bool m_shouldTerminate = false;

private:
  void threadLoop()
  {
    while (true)
    {
      std::packaged_task<std::vector<char>()> task;
      {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_mutex_condition.wait(lock, [&tasks = m_tasks, &shouldTerminate = m_shouldTerminate]{ return !tasks.empty() || shouldTerminate; });
        if (m_shouldTerminate)
        {
          return void();
        }
        task = std::move(m_tasks.front());
        m_tasks.pop();
      }
      task();
    }
  }
};

class MergeFileSort
{
public:
  MergeFileSort() : m_threadPool(std::thread::hardware_concurrency())
  {
  }
  MergeFileSort(int nThreads) : m_threadPool(nThreads)
  {
  }

  std::string sortFile(const std::string &path)
  {
    std::vector<std::string> fileNames = makeSortedFiles(path);
    std::string nameMergedFile = mergeFiles(std::move(fileNames));
    return nameMergedFile;
  }

private:
  void saveBlock(const std::string &name, std::vector<char> &&data)
  {
    std::cout << "File: " << name << " created." << " Size: " <<  data.size() << std::endl;
    std::ofstream stream(name, std::ofstream::binary);
    stream.write(data.data(), data.size());
    stream.close();
  }

  std::vector<std::string> makeSortedFiles(const std::string &fileName)
  {
    std::ifstream is(fileName, std::ifstream::binary);
    if (!is)
    {
      return {};
    }

    is.seekg(0, is.end);
    m_sizeFile = is.tellg();
    is.seekg(0, is.beg);
    m_fileName = fileName;

    int partsCount = std::ceil(static_cast<float>(m_sizeFile) / static_cast<float>(SIZE_FILE_PART));
    std::queue<std::future<std::vector<char>>> futures;
    for (int part = 0; part < partsCount; ++part)
    {
      std::vector<char> readData;

      if ((part * SIZE_FILE_PART) + SIZE_FILE_PART > m_sizeFile)
      {
        readData.resize(m_sizeFile - (part * SIZE_FILE_PART));
        is.read(&readData[0], m_sizeFile - (part * SIZE_FILE_PART));
      }
      else
      {
        readData.resize(SIZE_FILE_PART);
        is.read(&readData[0], SIZE_FILE_PART);
      }

      std::packaged_task<std::vector<char>()> task([data = std::move(readData)]() mutable
       {  
        std::sort(data.begin() , data.end());
        return data; });

      futures.push(task.get_future());
      m_threadPool.putTask(std::move(task));
    }
    is.close();

    int countTemp = 0;
    std::vector<std::string> fileNames;
    fileNames.reserve(futures.size());

    while (!futures.empty())
    {
      std::string name = "temp-" + std::to_string(countTemp) + ".bin";
      saveBlock(name, futures.front().get());
      futures.pop();
      fileNames.push_back(name);
      countTemp++;
    }
    return fileNames;
  }

  std::string mergeFiles(std::vector<std::string>&& fileNames)
  {
    
    int counterHierarchyMerged = 0;
    std::string fileNameWithoutPair = "";
    std::string resultName = "SORTED-" + m_fileName;
    while (fileNames.size() > 1)
    {
      size_t size = fileNames.size();
      int counterMergedFile = 0;
      std::vector<std::string> mergedFileNames;
      mergedFileNames.reserve(size / 2);
      std::vector<std::future<std::vector<char>>> futures;

      for (int i = 0; i < size; i += 2)
      {
        std::ifstream file1;
        std::ifstream file2;

        if (i + 1 >= size)
        {
          if (fileNameWithoutPair.empty())
          {
            fileNameWithoutPair = fileNames.at(i);
            fileNames.erase(fileNames.begin() + i);
            break;
          }
          else
          {
            file1.open(fileNames.at(i), std::ifstream::binary);
            file2.open(fileNameWithoutPair.c_str(), std::ifstream::binary);
            fileNames.push_back(fileNameWithoutPair);
            fileNameWithoutPair.clear();
          }
        }
        else
        {
          file1.open(fileNames.at(i), std::ifstream::binary);
          file2.open(fileNames.at(i + 1), std::ifstream::binary);
        }

        if (!file1 || !file2)
        {
          throw "Temp file open error.";
        }

        auto task = getMergeTask(std::move(file1), std::move(file2));
        futures.push_back(task.get_future());
        m_threadPool.putTask(std::move(task));

        counterMergedFile++;
      }

      while (!futures.empty())
      {
        std::string nameMergedFile;
        if (size <= 2)
        {
          std::remove(resultName.c_str());
          nameMergedFile = resultName;
          if(!fileNameWithoutPair.empty()){
          saveBlock(nameMergedFile, futures.back().get());
          futures.pop_back();
          std::ifstream streamFileNoPair(fileNameWithoutPair.c_str(), std::ifstream::binary );
          std::ifstream streamResult(nameMergedFile.c_str() ,  std::ifstream::binary );

          auto task = getMergeTask(std::move(streamFileNoPair) , std::move(streamResult));
          futures.emplace_back(task.get_future());
          m_threadPool.putTask(std::move(task));
          fileNames.push_back(fileNameWithoutPair);
          }
        }
        else
        {
          nameMergedFile = "merged-" + std::to_string(counterMergedFile--) +
                           " Hierarchy-" + std::to_string(counterHierarchyMerged) + ".bin";
        }
        saveBlock(nameMergedFile, futures.back().get());
        futures.pop_back();
        mergedFileNames.push_back(nameMergedFile);
      }

      for (const std::string &fileName : fileNames)
      {
        std::remove(fileName.c_str());
      }
      fileNames = mergedFileNames;
      counterHierarchyMerged++;
    }

   if( counterHierarchyMerged == 0 ){ // слияний не происходило, нужно переимновать файл и выдать его за результат
    std::rename(fileNames.front().c_str() , resultName.c_str() );
   }

    return resultName;
  }

  std::packaged_task<std::vector<char>()> getMergeTask(std::ifstream &&stream1, std::ifstream &&stream2)
  {
    return std::packaged_task<std::vector<char>()>([streamFile1 = std::move(stream1), streamFile2 = std::move(stream2)]() mutable
    {  
        std::vector<char> mergedData;
        mergedData.reserve(SIZE_FILE_PART * 2);

        typedef std::istreambuf_iterator<char> It;
        std::merge(It(streamFile1),It(),It(streamFile2),It(), std::back_inserter(mergedData));
      
        streamFile1.close();
        streamFile2.close();

        return mergedData; });
  }

private:
  size_t m_sizeFile;
  std::string m_fileName;
  static constexpr int SIZE_FILE_PART = 5000; //байты
  ThreadPool m_threadPool;
};

int main()
{
  static const int THREADS = 5;
  MergeFileSort fileSort(THREADS);
  std::cout << "5 threads available" << std::endl;
  std::cout << "Sorting file test_bin.bin..." << std::endl;
  std::string resultFileName = fileSort.sortFile("test_bin.bin");
  std::cout << "File: " << resultFileName << " generated!" << std::endl;
  std::cout << "finished";
  std::cin.get();
  return 0;
}