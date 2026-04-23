#include "fstream.h"
#include <vector>

// 磁盘事件类型：正常、故障、更换
enum class EventType {
  NORMAL,  // 正常：所有磁盘工作正常
  FAILED,  // 故障：指定磁盘发生故障（文件被删除）
  REPLACED // 更换：指定磁盘被更换（文件被清空）
};

class RAID5Controller {
private:
  std::vector<sjtu::fstream *> drives_; // 磁盘文件对应的 fstream 对象
  int blocks_per_drive_;               // 每个磁盘的块数
  int block_size_;                     // 每个块的大小
  int num_disks_;                      // 磁盘数
  std::vector<bool> failed_drives_;    // 标记哪些磁盘已故障
  int current_parity_disk_;            // 当前奇偶校验盘的位置

public:
  RAID5Controller(std::vector<sjtu::fstream *> drives, int blocks_per_drive,
                  int block_size = 4096) {
    drives_ = drives;
    blocks_per_drive_ = blocks_per_drive;
    block_size_ = block_size;
    num_disks_ = drives.size();
    failed_drives_.resize(num_disks_, false);
    current_parity_disk_ = 0;
  }

  /**
   * @brief 启动 RAID5 系统
   * @param event_type_ 磁盘事件类型
   * @param drive_id 发生事件的磁盘编号（如果是 NORMAL 则忽略）
   *
   * 如果是 FAILED，对应的磁盘文件会被删除。此时不可再对该文件进行读写。
   * 如果是 REPLACED，对应的磁盘文件会被清空（但文件依然存在）
   * 如果是 NORMAL，所有磁盘正常工作
   * 注：磁盘被替换之前不一定损坏。
   */
  void Start(EventType event_type_, int drive_id) {
    if (event_type_ == EventType::FAILED) {
      // 标记磁盘为故障状态
      if (drive_id >= 0 && drive_id < num_disks_) {
        failed_drives_[drive_id] = true;
      }
    } else if (event_type_ == EventType::REPLACED) {
      // 重置磁盘状态（恢复为正常）
      if (drive_id >= 0 && drive_id < num_disks_) {
        failed_drives_[drive_id] = false;
        // 将当前奇偶校验盘设置为新替换的磁盘
        current_parity_disk_ = drive_id;
      }
    }
    // 对于 NORMAL 事件，不需要特殊处理
  }

  void Shutdown() {
    // 关闭所有打开的文件，以防未定义行为发生。
    for (auto& drive : drives_) {
      if (drive && drive->is_open()) {
        drive->close();
      }
    }
  }

  // 计算给定块ID的奇偶校验盘位置
  int get_parity_disk(int block_id) {
    // RAID5使用轮转奇偶校验，奇偶校验盘位置随块ID变化
    return (current_parity_disk_ + block_id) % num_disks_;
  }

  // 计算给定块ID的数据盘数量（不包括奇偶校验盘）
  int get_data_disks_count() {
    return num_disks_ - 1;
  }

  // 计算给定块ID在数据盘中的位置
  std::pair<int, int> get_data_disk_location(int block_id) {
    int parity_disk = get_parity_disk(block_id);
    int data_disk_id = block_id % get_data_disks_count();
    
    // 如果数据盘ID大于等于奇偶校验盘位置，则需要跳过奇偶校验盘
    if (data_disk_id >= parity_disk) {
      data_disk_id++;
    }
    
    int block_on_disk = block_id / get_data_disks_count();
    return std::make_pair(data_disk_id, block_on_disk);
  }

  // 计算给定块ID的奇偶校验块位置
  std::pair<int, int> get_parity_block_location(int block_id) {
    int parity_disk = get_parity_disk(block_id);
    int block_on_disk = block_id / get_data_disks_count();
    return std::make_pair(parity_disk, block_on_disk);
  }

  // 读取指定磁盘的指定块
  bool read_block_from_disk(int disk_id, int block_on_disk, char *buffer) {
    if (disk_id < 0 || disk_id >= num_disks_ || failed_drives_[disk_id]) {
      return false;
    }
    
    if (!drives_[disk_id]->is_open()) {
      return false;
    }
    
    drives_[disk_id]->seekg(block_on_disk * block_size_);
    drives_[disk_id]->read(buffer, block_size_);
    return drives_[disk_id]->good();
  }

  // 写入指定磁盘的指定块
  bool write_block_to_disk(int disk_id, int block_on_disk, const char *data) {
    if (disk_id < 0 || disk_id >= num_disks_ || failed_drives_[disk_id]) {
      return false;
    }
    
    if (!drives_[disk_id]->is_open()) {
      return false;
    }
    
    drives_[disk_id]->seekp(block_on_disk * block_size_);
    drives_[disk_id]->write(data, block_size_);
    return drives_[disk_id]->good();
  }

  // 计算奇偶校验块（异或操作）
  void calculate_parity(char *parity_buffer, const std::vector<char*> &data_buffers, int buffer_size) {
    // 初始化为0
    for (int i = 0; i < buffer_size; i++) {
      parity_buffer[i] = 0;
    }
    
    // 对所有数据块进行异或
    for (auto data_buffer : data_buffers) {
      for (int i = 0; i < buffer_size; i++) {
        parity_buffer[i] ^= data_buffer[i];
      }
    }
  }

  void ReadBlock(int block_id, char *result) {
    // 检查块ID是否有效
    if (block_id < 0 || block_id >= Capacity()) {
      return;
    }
    
    // 获取数据盘位置
    auto data_location = get_data_disk_location(block_id);
    int data_disk_id = data_location.first;
    int block_on_disk = data_location.second;
    
    // 如果数据盘未故障，直接读取
    if (!failed_drives_[data_disk_id]) {
      read_block_from_disk(data_disk_id, block_on_disk, result);
      return;
    }
    
    // 数据盘故障，需要从其他盘恢复数据
    // 读取同一组的所有其他数据块和奇偶校验块
    std::vector<char*> data_buffers;
    std::vector<int> data_disk_ids;
    
    // 收集同一组的所有数据块（不包括故障盘）
    for (int i = 0; i < num_disks_; i++) {
      if (i != data_disk_id && i != get_parity_disk(block_id) && !failed_drives_[i]) {
        char *buffer = new char[block_size_];
        if (read_block_from_disk(i, block_on_disk, buffer)) {
          data_buffers.push_back(buffer);
          data_disk_ids.push_back(i);
        } else {
          delete[] buffer;
        }
      }
    }
    
    // 读取奇偶校验块
    auto parity_location = get_parity_block_location(block_id);
    int parity_disk_id = parity_location.first;
    char *parity_buffer = new char[block_size_];
    bool parity_read = false;
    
    if (!failed_drives_[parity_disk_id]) {
      parity_read = read_block_from_disk(parity_disk_id, block_on_disk, parity_buffer);
    }
    
    // 恢复数据：如果能读取奇偶校验块，则用奇偶校验块异或所有其他数据块
    // 如果奇偶校验块也故障，则无法恢复
    if (parity_read) {
      // 初始化结果为奇偶校验块
      for (int i = 0; i < block_size_; i++) {
        result[i] = parity_buffer[i];
      }
      
      // 异或所有其他数据块
      for (auto data_buffer : data_buffers) {
        for (int i = 0; i < block_size_; i++) {
          result[i] ^= data_buffer[i];
        }
      }
    } else {
      // 无法恢复数据，返回0
      for (int i = 0; i < block_size_; i++) {
        result[i] = 0;
      }
    }
    
    // 释放内存
    for (auto buffer : data_buffers) {
      delete[] buffer;
    }
    delete[] parity_buffer;
  }

  void WriteBlock(int block_id, const char *data) {
    // 检查块ID是否有效
    if (block_id < 0 || block_id >= Capacity()) {
      return;
    }
    
    // 获取数据盘和奇偶校验盘位置
    auto data_location = get_data_disk_location(block_id);
    int data_disk_id = data_location.first;
    int block_on_disk = data_location.second;
    
    auto parity_location = get_parity_block_location(block_id);
    int parity_disk_id = parity_location.first;
    
    // 如果数据盘未故障，直接写入数据块
    if (!failed_drives_[data_disk_id]) {
      write_block_to_disk(data_disk_id, block_on_disk, data);
    }
    
    // 重新计算并写入奇偶校验块
    // 读取同一组的所有数据块
    std::vector<char*> data_buffers;
    
    for (int i = 0; i < num_disks_; i++) {
      if (i != parity_disk_id) {  // 所有非奇偶校验盘
        char *buffer = new char[block_size_];
        
        if (i == data_disk_id && !failed_drives_[data_disk_id]) {
          // 如果是当前要写入的数据盘，使用新数据
          for (int j = 0; j < block_size_; j++) {
            buffer[j] = data[j];
          }
        } else if (!failed_drives_[i]) {
          // 如果是其他正常的数据盘，从磁盘读取
          if (read_block_from_disk(i, block_on_disk, buffer)) {
            // Successfully read
          } else {
            // 如果读取失败，用0填充
            for (int j = 0; j < block_size_; j++) {
              buffer[j] = 0;
            }
          }
        } else {
          // 如果磁盘故障，用0填充
          for (int j = 0; j < block_size_; j++) {
            buffer[j] = 0;
          }
        }
        
        data_buffers.push_back(buffer);
      }
    }
    
    // 计算新的奇偶校验块
    char *new_parity = new char[block_size_];
    calculate_parity(new_parity, data_buffers, block_size_);
    
    // 如果奇偶校验盘未故障，写入新的奇偶校验块
    if (!failed_drives_[parity_disk_id]) {
      write_block_to_disk(parity_disk_id, block_on_disk, new_parity);
    }
    
    // 释放内存
    for (auto buffer : data_buffers) {
      delete[] buffer;
    }
    delete[] new_parity;
  }

  int Capacity() {
    // 返回磁盘阵列能写入的块的数量（你无需改动此函数）
    return (num_disks_ - 1) * blocks_per_drive_;
  }
};