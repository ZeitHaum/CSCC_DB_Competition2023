/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "ix_defs.h"
#include "common/common.h"
#include "common/context.h"
#include <queue>
#include "transaction/txn_defs.h"

class LogManager;

// #define ENABLE_LOCK_CRABBING
// #define DEBUG_IX_INDEX_HANDLE

enum class Operation { FIND = 0, INSERT = 1, DELETE = 2};  // 三种操作：查找、插入、删除

static const bool binary_search = false;

inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_BIGINT: {
            long long biga = *(long long *)a;
            long long bigb = *(long long *)b;
            return (biga < bigb) ? -1 : ((biga > bigb) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
            return memcmp(a, b, col_len);

        case TYPE_DATETIME:
            return memcmp(a, b, col_len);

        default:
            throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens) {
    int offset = 0;
    for(size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
    friend class IxIndexHandle;
    friend class IxScan;

   private:
    const IxFileHdr *file_hdr;      // 节点所在文件的头部信息
    Page *page;                     // 存储节点的页面
    IxPageHdr *page_hdr;            // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                     // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                      // page->data的第三部分，指针指向首地址
    BufferPoolManager* buffer_pool_manager;

   public:

    IxNodeHandle() = default;

    IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_, BufferPoolManager* buffer_pool_manager_) : file_hdr(file_hdr_), page(page_), buffer_pool_manager(buffer_pool_manager_){
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    //判断是否安全
    bool is_safe(Operation op){
        if(op == Operation::FIND){
            return true;
        }
        else if(op == Operation::INSERT){
            return this->get_num_vals() + 1 < this->get_max_size();
        }
        else{
            return this->get_num_vals() - 1 >= this->get_min_size();
        }
    }

    void unpin_node(bool is_dirty){
        this->buffer_pool_manager->unpin_page(this->page->get_page_id(), is_dirty);
    }

    int is_leaf_head(){ return this->get_page_no() == IX_LEAF_HEADER_PAGE;}

    int get_size() const { return page_hdr->num_key; }

    void set_size(int size) { page_hdr->num_key = size; }

    int get_max_size() { return file_hdr->btree_order_ + 1; }

    int get_min_size() { return get_max_size() / 2; }

    int key_at(int i) { return *(int *)get_key(i); }

    /* 得到第i个孩子结点的page_no */
    page_id_t value_at(int i) { return get_rid(i)->page_no; }

    page_id_t get_page_no() { return page->get_page_id().page_no; }

    PageId get_page_id() { return page->get_page_id(); }

    int get_num_vals() {return (this->is_leaf_page())? this->page_hdr->num_key : this->page_hdr->num_key+1; }

    page_id_t get_next_leaf() { return page_hdr->next_leaf; }

    page_id_t get_prev_leaf() { return page_hdr->prev_leaf; }

    page_id_t get_parent_page_no() { return page_hdr->parent; }

    bool is_leaf_page() const { return page_hdr->is_leaf; }

    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

    void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

    void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }

    char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

    Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }

    void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); }

    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }

    int lower_bound(const char *target) const;

    int binary_search(const char *target) const ;

    int upper_bound(const char *target) const;

    void insert_pairs(int key_pos, int rid_pos, const char *key, const Rid *rid, int n);

    page_id_t internal_lookup(const char *key);

    bool leaf_lookup(const char *key, Rid **value);

    int insert(const char *key, const Rid &value);

    // 用于在结点中的指定位置插入单个键值对
    void insert_pair(int key_pos, int rid_pos, const char *key, const Rid &rid) { insert_pairs(key_pos, rid_pos,  key, &rid, 1); }

    void erase_pair(int pos);

    int remove(const char *key);

    /**
     * @brief used in internal node to remove the last key in root node, and return the last child
     *
     * @return the last child
     */
    page_id_t remove_and_return_only_child() {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(IxNodeHandle *child) {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            if (get_rid(rid_idx)->page_no == child->get_page_no()) {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }

    //add func
    void key_swap(int a, int b) {
        int len = file_hdr->col_tot_len_;
        char mid[len];
        memcpy(mid, keys + a*len, len);
        memcpy(keys + a*len, keys + b*len, len);
        memcpy(keys + b*len, mid, len);
    };

    void rid_swap(int a, int b) {
        Rid mid;
        mid.copy(rids[a]);
        rids[a].copy(rids[b]);
        rids[b].copy(mid);
    };

    //打印所有数据
    std::vector<std::vector<Value>>  get_all_keys(){
        std::vector<std::vector<Value>> ret;
        char* now_key = this->keys;
        for(int i = 0;i<this->get_size();i++){
            ret.emplace_back(std::vector<Value>());
            for(int j = 0; j < (int)this->file_hdr->col_types_.size(); j++){
                Value v = Value();
                v.type = this->file_hdr->col_types_[j];
                v.get_val_from_raw(now_key, this->file_hdr->col_lens_[j]);
                now_key += this->file_hdr->col_lens_[j];
                ret[i].emplace_back(v);
            }
        }
        return ret;
    }

    int father_lookup(const char *key) {
        int pos = lower_bound(key);
        if(pos == get_size()) {
            return pos;
        }
        int tmp = ix_compare(key, get_key(pos), file_hdr->col_types_, file_hdr->col_lens_);

        if(tmp == 0) {
            return pos+1;
        }
        else if(tmp<0) {
            return pos;
        }
        else {
            assert(0);
            return -1;
        }
    }
};


struct node_guard{
    IxNodeHandle* node = nullptr;
    bool is_dirty = true;
    node_guard(IxNodeHandle* node_, bool is_dirty_) : node(node_), is_dirty(is_dirty_){}

    ~node_guard(){
        if(node != nullptr){
            node->unpin_node(is_dirty);
            delete node;
            node = nullptr;
        }
    }
};


/* B+树 */
class IxIndexHandle {
    friend class IxScan;
    friend class IxManager;

   private:
    BufferPoolManager *buffer_pool_manager_;
    DiskManager *disk_manager_;
    int fd_;                                    // 存储B+树的文件
    IxFileHdr* file_hdr_;                       // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）

   public:
    std::mutex root_latch_;
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

    ~IxIndexHandle(){
        delete file_hdr_;
    }

    void lock_crabbing(Operation op, Transaction* txn, IxNodeHandle* node){
        if(txn==nullptr){
            //啥也不做.
            return;
        }
        lock(op, node->page);
        if(node->is_safe(op)){
            unlock_all_latch(txn);
        }
        //添加到锁表
        txn->append_index_latch_page_set(node->page, static_cast<int>(op));
    }

    void unlock_all_latch(Transaction* txn){
        if(txn==nullptr){
            //啥也不做.
            return;
        }
        auto index_latch_pages = txn->get_index_latch_page_set();
        //先unlock
        for(auto iter = index_latch_pages->begin(); iter!= index_latch_pages->end(); iter++){
            unlock(static_cast<Operation>(iter->second), iter->first);
        }
        //再删除
        index_latch_pages->clear();
    }

    void lock(Operation op, Page* page){
        if(op == Operation::FIND){
            lock_read(page);
        }
        else{
            lock_write(page);
        }
    }

    void unlock(Operation op, Page* page){
        if(op == Operation::FIND){
            unlock_read(page); 
        }
        else{
            unlock_write(page);
        }
    }

    void lock_write(Page* page) { 
        page->add_pin_count();
        page->rw_latch_.lock_write();
    }


    void unlock_write(Page* page) { 
        page->rw_latch_.unlock_write();
        this->buffer_pool_manager_->unpin_page(page->get_page_id(), false);
    }


    void lock_read(Page* page) { 
        page->add_pin_count();
        page->rw_latch_.lock_read();
    }


    void unlock_read(Page* page) { 
        page->rw_latch_.unlock_read();
        this->buffer_pool_manager_->unpin_page(page->get_page_id(), false);
    }

    // for search
    bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);

    bool is_key_exists(const char *key, Transaction *transaction);

    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                 bool find_first = false);

    // for insert
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction);

    IxNodeHandle *split(IxNodeHandle *node);

    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

    // for delete
    bool delete_entry(const char *key, const Rid& rid, Transaction *transaction);

    bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                bool *root_is_latched = nullptr);
    bool adjust_root(IxNodeHandle *old_root_node);

    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                  Transaction *transaction, bool *root_is_latched);

    Iid lower_bound(const char *key, Context* context_);

    Iid binary_search(const char *key, Context* context_);

    Iid upper_bound(const char *key, Context* context_);

    Iid leaf_end() const;

    Iid leaf_begin() const;

    int first_ind_key() const;

    int last_ind_key() const;

    bool find_ind_key_at(const Iid& iid_, int& ret) const;

    IxFileHdr* getFileHdr(){
        return this->file_hdr_;
    }

    BufferPoolManager * getBufferPoolManager(){
        return this->buffer_pool_manager_;
    }

    int getFd(){ return this->fd_; }

    Rid get_rid(const Iid &iid) const;

    page_id_t get_first_leaf() { return file_hdr_->first_leaf_; }

    page_id_t get_last_leaf() { return file_hdr_->last_leaf_; }

    Iid get_start_pos() {
        return Iid({get_first_leaf(), 0});
    }

    Iid get_end_pos() {
        IxNodeHandle * last_leaf = fetch_node(get_last_leaf());
        Iid ret;
        ret.page_no = get_last_leaf();
        ret.slot_no = last_leaf->get_size();
        buffer_pool_manager_->unpin_page(last_leaf->get_page_id(), false);
        delete last_leaf;
        return ret;
    }

    std::vector<std::vector<page_id_t>> get_bp_tree(){
        std::vector<std::vector<page_id_t>>ret(file_hdr_->num_pages_, std::vector<page_id_t>(0));
        std::queue<page_id_t> q;
        q.push(file_hdr_->root_page_);
        while(!q.empty()){
            page_id_t qf = q.front();
            q.pop();
            IxNodeHandle* qf_node = fetch_node(qf);
            if(qf_node->is_leaf_page()){
                buffer_pool_manager_->unpin_page({fd_, qf}, false);
                delete qf_node;    
                continue;
            }
            for(int i = 0;i<qf_node->get_num_vals();i++){
                ret[qf].emplace_back(qf_node->rids[i].page_no);
                q.push(qf_node->rids[i].page_no);
            }
            buffer_pool_manager_->unpin_page({fd_, qf}, false);
            delete qf_node;    
        }
        return ret;
    }

    IxNodeHandle* get_prev_node(IxNodeHandle* node){
        if(node->is_leaf_page()){
            IxNodeHandle* prev_node =  fetch_node(node->get_prev_leaf());
            if(prev_node->is_leaf_head()){
                buffer_pool_manager_->unpin_page(prev_node->get_page_id(), false);
                delete prev_node;
                return nullptr;
            }
            else{
                return prev_node;
            }
        }
        else{
            IxNodeHandle* parent_node = fetch_node(node-> get_parent_page_no());
            int pos = -1;
            if(node->get_size() == 0){
                for(int i = 0;i<parent_node->get_num_vals();i++){
                    #ifdef DEBUG_IX_INDEX_HANDLE
                        page_id_t page_no = parent_node->get_rid(i)->page_no;
                    #endif
                    if(parent_node->get_rid(i)->page_no == node->get_page_no()){
                        pos = i;
                        break;
                    }
                }
            }
            else{
                pos = parent_node->father_lookup(node->get_key(0));
            }
            if(pos==0){
                buffer_pool_manager_->unpin_page(parent_node->get_page_id(), false);
                delete parent_node;
                return nullptr;
            }
            else{
                IxNodeHandle* prev_node = fetch_node(parent_node->get_rid(pos - 1)->page_no);
                buffer_pool_manager_->unpin_page(parent_node->get_page_id(), false);
                delete parent_node;
                return prev_node;
            }
        }
    }

    IxNodeHandle* get_next_node(IxNodeHandle* node){
        if(node->is_leaf_page()){
            if(node->get_page_no() == file_hdr_ -> last_leaf_){
                return nullptr;
            }
            else{
                IxNodeHandle* next_node =  fetch_node(node->get_next_leaf());
                return next_node;
            }
        }
        else{
            IxNodeHandle* parent_node = fetch_node(node-> get_parent_page_no());
            int pos = -1;
            if(node->get_size()>0){
                pos = parent_node->father_lookup(node->get_key(0));
            }
            else{
                for(int i = 0;i<parent_node->get_num_vals();i++){
                    if(parent_node->get_rid(i)->page_no == node->get_page_no()){
                        pos = i;
                        break;
                    }
                }
            }
            assert(pos <= parent_node->get_num_vals() - 1);
            if(pos==parent_node->get_num_vals() - 1){
                buffer_pool_manager_->unpin_page(parent_node->get_page_id(), false);
                delete parent_node;
                return nullptr;
            }
            else{
                IxNodeHandle* next_node = fetch_node(parent_node->get_rid(pos + 1)->page_no);
                buffer_pool_manager_->unpin_page(parent_node->get_page_id(), false);
                delete parent_node;
                return next_node;
            }
        }
    }
    // void lock_root(txn_id_t txn_id){
    //     //检查加锁, try_lock将会直接加锁
    //     this->root_latch_.lock();
    // }

    // void lock_root(){
    //     this->root_latch_.lock();
    // }

    // void unlock_root(){
    //     assert(this->root_latch_.try_lock() == false);// 必须已经加锁的才能释放锁
    //     this->root_latch_.unlock();
    // }

   private:
    // 辅助函数
    void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }

    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    // for get/create node
    IxNodeHandle *fetch_node(int page_no) const;

    IxNodeHandle *create_node();

    // for maintain data structure
    void maintain_parent(IxNodeHandle *node, const char* old_key, const char* new_key);

    void erase_leaf(IxNodeHandle *leaf);

    void release_node_handle(IxNodeHandle &node);

    void maintain_child(IxNodeHandle *node, int child_idx);

};

//释放所有锁
struct lock_txn_guard{
    Transaction* txn;
    IxIndexHandle* ih_;
    lock_txn_guard(Transaction* txn, IxIndexHandle* ih): txn(txn), ih_(ih) {}
    ~lock_txn_guard(){
        ih_->unlock_all_latch(txn);
    }
};