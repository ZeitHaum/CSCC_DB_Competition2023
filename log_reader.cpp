#include <iostream>
#include <fstream>

int main() {
    std::ifstream file("./testdb/db.log", std::ios::binary); 
    if (file) {
        // 获取文件大小
        file.seekg(0, std::ios::end);
        std::streampos fileSize = file.tellg();
        file.seekg(0, std::ios::beg);

        // 检查偏移量是否有效
        std::streampos offset = 112784 + 4; // 假设偏移量为 10
        if (offset + 4 > fileSize) {
            std::cout << "Invalid offset." << std::endl;
            return 1;
        }

        // 定位到指定偏移量
        file.seekg(offset, std::ios::beg);

        // 读取四个字节并转为 int
        int value = 0;
        file.read(reinterpret_cast<char*>(&value), sizeof(int));

        std::cout << "Value: " << value << std::endl;

        file.close();
    } else {
        std::cout << "Failed to open the file." << std::endl;
    }

    return 0;
}
