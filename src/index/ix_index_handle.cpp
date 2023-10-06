/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include "ix_scan.h"
#include "recovery/log_manager.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较

    int num_key = get_size();

    int l = -1, r = num_key;

    while(r-l>1){
        int mid = l + (r - l)/2;
        auto check = [&](){
            const char* mid_ptr = this->get_key(mid);
            if(ix_compare(mid_ptr, target, this->file_hdr->col_types_, this->file_hdr->col_lens_)<0){
                // mid < target.
                return 1;
            }
            else{
                // mid >= target.
                return 2;
            }
        };
        if(check()==1){
            l = mid; 
        }
        else{
            r = mid;
        }
    }

    return r;
}


/**
 * @brief 在当前node中查找第一个==target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::binary_search(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较

    int num_key = get_size();

    int l = -1, r = num_key;

    while(r-l>1){
        int mid = l + (r - l)/2;
        auto check = [&](){
            const char* mid_ptr = this->get_key(mid);
            if(ix_compare(mid_ptr, target, this->file_hdr->col_types_, this->file_hdr->col_lens_)<0){
                // mid < target.
                return 1;
            }
            else{
                // mid >= target.
                return 2;
            }
        };
        if(check()==1){
            l = mid; 
        }
        else{
            r = mid;
        }
    }

    if(r == get_size() || ix_compare(this->get_key(r), target, this->file_hdr->col_types_, this->file_hdr->col_lens_)!=0) {
        r = -1;
    }
    
    return r;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较

    int num_key = get_size();

    #ifdef DEBUG_IX_INDEX_HANDLE
        int num = *(int *)target;
        int id = *(int *)(target + 4);
    #endif

    int l = -1, r = num_key;

    while(r-l>1){
        int mid = l + (r - l)/2;
        auto check = [&](){
            const char* mid_ptr = this->get_key(mid);
            #ifdef DEBUG_IX_INDEX_HANDLE
                int num__ = *(int *)mid_ptr;
                int id__ = *(int *)(mid_ptr + 4);
            #endif
            if(ix_compare(mid_ptr, target, this->file_hdr->col_types_, this->file_hdr->col_lens_)<=0){
                // mid <= target.
                return 1;
            }
            else{
                // mid > target.
                return 2;
            }
        };
        if(check()==1){
            l = mid; 
        }
        else{
            r = mid;
        }
    }

    return r;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    if(!page_hdr->is_leaf) {
        assert(0&&"This is not a leaf!");
    }
    int pos = lower_bound(key);
    if(pos == get_size()) {
        return false;
    }
    if(ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) != 0) {
        return false;
    }

    (*value)->page_no = get_rid(pos)->page_no;
    (*value)->slot_no = get_rid(pos)->slot_no;
    return true;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int pos = lower_bound(key);
    if(pos == get_size()) {
        return get_rid(pos)->page_no;
    }
    int tmp = ix_compare(key, get_key(pos), file_hdr->col_types_, file_hdr->col_lens_);

    if(tmp == 0) {
        return get_rid(pos+1)->page_no;
    }
    else if(tmp<0) {
        return get_rid(pos)->page_no;
    }
    else {
        assert(0);
        return -1;
    }
}


/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int key_pos, int rid_pos, const char *key, const Rid *rid, int n) {
    // Todo:
    //未考虑分裂

    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量

    //pos 是 Key的位置
    // 1. 判断pos的合法性
    if(key_pos<0 || key_pos>get_size()) {
        throw InternalError("IxNodeHandle::insert pos overflow");
    }

    if(rid_pos<0 || rid_pos>get_num_vals()){
        throw InternalError("IxNodeHandle::insert pos overflow");
    }

    if(is_leaf_page() && get_size() + n > get_max_size()) {
        throw InternalError("IxNodeHandle::insert pos overflow");
    }
    else if( get_size() + n > get_max_size() - 1 ) {
        throw InternalError("IxNodeHandle::insert pos overflow");
    }

    //先挪
    for(int i = get_size() - 1;i>=key_pos;i--){
        key_swap(i, i+n);
    }

    for(int i = get_num_vals() - 1; i>=rid_pos; i--){
        rid_swap(i, i+n);
    }

    //set Key
    int offset = 0;
    for(int i = key_pos; i<key_pos+n; i++){
        set_key(i, key + offset);
        offset += file_hdr->col_tot_len_;
    }

    //set Rid
    for(int i = rid_pos; i<rid_pos+n;i++){
        set_rid(i, *(rid + (i - rid_pos)));
    }

    this->page_hdr ->num_key += n;

    // // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // for(int i=0;i<n;i++) {
    //     for(int j = get_size(); j > pos; j--) {
    //         key_swap(j-1,j);
    //     }
    //     set_key(pos, key + i*file_hdr->col_tot_len_);

    //     //4. 更新当前节点的键数量
    //     page_hdr->num_key++;
    // }

    // // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // if(!page_hdr->is_leaf) {
    //     for(int i=0;i<n;i++) {
    //         for(int j = get_size() + 1; j > pos + 1; j--) {
    //             rid_swap(j-1,j);
    //         }
    //         set_rid(pos, rid[i]);
    //     }
    // }
    // else {
    //     for(int i=0;i<n;i++) {
    //         for(int j = get_size(); j > pos; j--) {
    //             rid_swap(j-1,j);
    //         }
    //         set_rid(pos, rid[i]);
    //     }
    // }
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(const char *key, const Rid &value) {
    // Todo:
    //可以插满，上层接口判断是否要分裂
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量

    int pos = lower_bound(key);

    if(pos != page_hdr->num_key && ix_compare(key, get_key(pos), file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        throw IndexInsertDuplicatedError();
    }   

    //交换keys数组
    for(int i = get_size(); i > pos; i--) {
        key_swap(i-1, i);
    }
    //将key插入keys
    set_key(pos, key);

    //对于叶子和非叶子，rid插入位置不同
    if(page_hdr->is_leaf) {
        for(int i=page_hdr->num_key;i>pos;i--) {
            rid_swap(i-1, i);
        }
        set_rid(pos, value);
    }
    else {
        for(int i=page_hdr->num_key+1;i>pos+1;i--) {
            rid_swap(i-1, i);
        }
        set_rid(pos+1, value);
    }
    page_hdr->num_key++;

    return page_hdr->num_key;
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量
    if(pos < 0 || pos >= get_size()) {
        return ;
    }

    // 1. 删除该位置的key
    memset(get_key(pos), 0, file_hdr->col_tot_len_);

    // 2. 删除该位置的rid
    if(is_leaf_page()) {
        // get_rid(pos + 1)->page_no = INVALID_PAGE_ID;
        // get_rid(pos + 1)->slot_no = 0;
        for(int i = pos+1; i<this->get_size();i++){
            key_swap(i, i -1 );
        }
        for(int i = pos+1; i<this->get_num_vals();i++){
            rid_swap(i, i-1);
        }
    }
    else {
        // get_rid(pos)->page_no = INVALID_PAGE_ID;
        // get_rid(pos)->slot_no = 0;
        for(int i = pos+1; i<this->get_size();i++){
            key_swap(i, i - 1);
        }
        for(int i = pos+2; i<this->get_num_vals();i++){
            rid_swap(i, i - 1);
        }
    }

    // 3. 更新结点的键值对数量
    page_hdr->num_key--;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(const char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量

    int pos = lower_bound(key);
    //判断位置是否合法

    if(pos<0 || pos == page_hdr->num_key){
        //键值不存在
        throw InternalError("indexhandle.remove(): Error, cannot find key.");
    }
    if(pos != page_hdr->num_key && ix_compare(key, get_key(pos), file_hdr->col_types_, file_hdr->col_lens_) != 0) {
        //键值不存在
        throw InternalError("indexhandle.remove(): Error, cannot find key.");
    }

    //键值存在
    this->erase_pair(pos);

    return this->page_hdr->num_key;
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
        :buffer_pool_manager_(buffer_pool_manager),
        disk_manager_(disk_manager), 
        fd_(fd)
    {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char* buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    delete[] buf;
    
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    // int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, file_hdr_->num_pages_);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                            Transaction *transaction, bool find_first) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点

    IxNodeHandle* curr_node = this->fetch_node(this->file_hdr_->root_page_);
    
    #ifdef DEBUG_IX_INDEX_HANDLE
        auto gdb_unused_ret = curr_node->get_all_keys();
    #endif

    while(!curr_node->page_hdr->is_leaf){
        //查找子页面号
        node_guard ng(curr_node, false);
        //判断curr_node是否安全，如果安全，释放所有父亲结点的锁
        #ifdef ENABLE_LOCK_CRABBING
            lock_crabbing(operation, transaction, curr_node);
        #endif
        page_id_t child_page_no = curr_node->internal_lookup(key);
        curr_node = fetch_node(child_page_no);
        #ifdef DEBUG_IX_INDEX_HANDLE
            auto gdb_unused_ret = curr_node->get_all_keys();
        #endif
    }

    return std::make_pair(curr_node, false);
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中+
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁

    IxNodeHandle* leaf_node = find_leaf_page(key, Operation::FIND, transaction, false).first;
    #ifdef ENABLE_LOCK_CRABBING 
        lock_txn_guard ltg(transaction, this);
    #endif
    node_guard ng(leaf_node, false);

    Rid* ret = new Rid();

    if(!leaf_node->leaf_lookup(key, &ret)){
        delete ret;
        return false;
    }

    result->emplace_back(*ret);
    delete ret;
    return true;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())

    IxNodeHandle *new_node = create_node();

    //左右节点value数
    int r = node->get_num_vals()/2;
    int l = node->get_num_vals() - r;

    //需要初始化新节点的page_hdr内容
    new_node->page_hdr->num_key = 0;

    if(node->page_hdr->is_leaf) {
        //如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
        if(node->get_page_no() == file_hdr_->last_leaf_){
            file_hdr_ -> last_leaf_ = new_node->get_page_no();
        }
        new_node->page_hdr->is_leaf = true;
        new_node->page_hdr->next_leaf = node->page_hdr->next_leaf;
        new_node->page_hdr->prev_leaf = node->page->get_page_id().page_no;
        node->page_hdr->next_leaf = new_node->page->get_page_id().page_no;

        if(new_node->get_page_no()!= file_hdr_->last_leaf_) {
            IxNodeHandle* new_next_node = fetch_node(new_node->get_next_leaf());
            node_guard ng(new_next_node, true);
            new_next_node ->set_prev_leaf(new_node->get_page_no());
        }

        //为新节点分配键值对，insert_pairs会增加num_key
        new_node->insert_pairs(0, 0, node->keys + l * file_hdr_->col_tot_len_, node->rids + l, r);
    }
    else {
        //为新节点分配键值对
        new_node->page_hdr->is_leaf = false;
        new_node->insert_pairs(0, 0, node->keys + (node->get_size() - r) * file_hdr_->col_tot_len_, node->rids + (node->get_num_vals() - r), r);
    }

    //更新旧节点的键值对数记录
    node->page_hdr->num_key = node->is_leaf_page()? l : l - 1;

    //如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息
    if(!new_node->page_hdr->is_leaf) {
        for(int i=0;i<new_node->get_num_vals() -1 ;i++) { // 还有个key没有挪上去，所以虚高
            maintain_child(new_node, i);
        }
    }

    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key_, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page


    //TODO:考虑transaction
    PageId parent_page_id = {this->fd_,-1};
    IxNodeHandle* parent_node = nullptr;

    char* key = new char[file_hdr_->col_tot_len_];
    memcpy(key, key_, file_hdr_->col_tot_len_);

    //维护new_node的数据
    //先把new_nodekeys往前挪
    if(!new_node->is_leaf_page()){
        for(int i = 1;i<=new_node->page_hdr->num_key;i++){//new_node的有效keys:[0, numkey-2](内部结点); [0, numkey-1](中间结点)
            new_node->key_swap(i,i-1);
        }
        new_node->page_hdr->num_key --;
    }

    //获取原结点（old_node）的父亲结点(不维护new_node和old_node的信息)
    if(old_node->is_root_page()){
        //新建根节点
        buffer_pool_manager_->new_page(&parent_page_id);
        buffer_pool_manager_->unpin_page(parent_page_id, false);
        parent_node = fetch_node(parent_page_id.page_no);
        //维护根节点元数据
        //filehdr, page:fetch_node
        parent_node->page_hdr->is_leaf = false;
        parent_node->page_hdr->num_key = 0;
        parent_node->page_hdr->parent = INVALID_PAGE_ID;
        //page_hdr其余是叶节点的元数据，不用管

        //维护file_hdr的元数据
        file_hdr_->num_pages_++;
        file_hdr_->root_page_ = parent_node->get_page_no();

        //rid插入一个指向old_node的rid
        parent_node->set_rid(0, {old_node->get_page_id().page_no, 0});// slot_no = 0
    }
    else{
        parent_page_id.page_no =  old_node->get_parent_page_no();
        parent_node = fetch_node(old_node->get_parent_page_no());
    }
    node_guard parent_ng(parent_node, true);

    //维护parent
    if(old_node->is_root_page()){
        old_node->page_hdr->parent = parent_node->get_page_no();
    }
    new_node->page_hdr->parent = old_node->get_parent_page_no();

    //获取key对应的rid(一定在new_node中)，插入
    Rid rid = {new_node->get_page_no(), 0}; // slot_no 为第一个大于key的位置
    parent_node->insert(key, rid);

    //若已满需要递归
    assert(!parent_node->is_leaf_page());
    if(!parent_node->is_leaf_page() && parent_node->get_num_vals()>file_hdr_->btree_order_ )//非叶子节点key缺了一项,需要+1
    {
        IxNodeHandle *new_node_ = split(parent_node);
        node_guard new_ng(new_node_, true);
        if(new_node_ ->is_leaf_page() && old_node->get_page_no() == file_hdr_->last_leaf_) {
            file_hdr_->last_leaf_ = new_node_->get_page_no();
        }
        //递归
        insert_into_parent(parent_node, new_node_->get_key(0), new_node_, nullptr);
    }

    delete[] key;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁

    //未处理锁
    // 1. 查找key值应该插入到哪个叶子节点
    auto leaf_page = find_leaf_page(key, Operation::INSERT, transaction, false);
    #ifdef ENABLE_LOCK_CRABBING
        lock_txn_guard ltg(transaction, this);
    #endif
    node_guard ng(leaf_page.first, true);

    page_id_t page_no = leaf_page.first->page->get_page_id().page_no;
    // 2. 在该叶子节点中插入键值对
    int num_keys;
    char * old_key = nullptr;
    bool is_ins_first = ix_compare(key, leaf_page.first->get_key(0), file_hdr_->col_types_, file_hdr_->col_lens_) < 0;
    if(is_ins_first){
        old_key = new char[file_hdr_->col_tot_len_];
        memcpy(old_key, leaf_page.first->get_key(0), file_hdr_->col_tot_len_);
    }
    try{
        num_keys = leaf_page.first->insert(key, value);
    }
    catch(IndexInsertDuplicatedError& e){
        if(is_ins_first){
            delete[] old_key;
        }
        throw e;
    }

    if(is_ins_first){
        maintain_parent(leaf_page.first, old_key, leaf_page.first->get_key(0));
        delete[] old_key;
    }

    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    if(num_keys > file_hdr_->btree_order_ ) {
        IxNodeHandle *new_node = split(leaf_page.first);
        node_guard new_ng(new_node, true);
        if(page_no == file_hdr_->last_leaf_) {
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }
        //递归
        insert_into_parent(leaf_page.first, new_node->get_key(0), new_node, nullptr);
    }
    #ifdef DEBUG_IX_INDEX_HANDLE
        auto show_debug1 = get_bp_tree();
    #endif

    if(transaction != nullptr) {
        // file_hdr_->file_lsn_ = buffer_pool_manager_->get_log_manager()->add_ix_insert_log_record(transaction->get_transaction_id(), key, file_hdr_->col_tot_len_, value, disk_manager_->get_file_name(fd_));
    }
    return page_no;
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁

    //TODO: 事务

    //TODO:并发
    IxNodeHandle* to_del_node = find_leaf_page(key, Operation::DELETE, transaction, false).first;
    #ifdef ENABLE_LOCK_CRABBING
        lock_txn_guard ltg(transaction, this);
    #endif
    node_guard to_del_ng(to_del_node, true);
    
    #ifdef DEBUG_IX_INDEX_HANDLE
        auto show_debug232 = get_bp_tree();
        auto show_128 = to_del_node->get_all_keys();
    #endif

    try {
        //删除键(2操作交由remove处理)
        bool is_del_first = ix_compare(key, to_del_node->get_key(0), file_hdr_->col_types_, file_hdr_->col_lens_) == 0;
        char * old_key = nullptr;
        if(is_del_first){
            old_key = new char[file_hdr_->col_tot_len_];
            memcpy(old_key, to_del_node->get_key(0),  file_hdr_->col_tot_len_);
        }
        to_del_node->remove(key);

        //特判main_parent
        // int num_vals = to_del_node->get_num_vals();
        if(is_del_first){
            maintain_parent(to_del_node, old_key, to_del_node->get_key(0));
            delete[] old_key;
        }

        //TODO:处理CoalesceOrRedistribute Return 值, 处理并发
        #ifdef DEBUG_IX_INDEX_HANDLE
            auto show_int = *(int*)key;
        #endif
        coalesce_or_redistribute(to_del_node, transaction, nullptr);
    }
    catch(const InternalError& e){
        return false;//删除失败
    }
    #ifdef DEBUG_IX_INDEX_HANDLE
        show_debug232 = get_bp_tree();
    #endif

    if(transaction != nullptr) {
        // file_hdr_->file_lsn_ = buffer_pool_manager_->get_log_manager()->add_ix_delete_log_record(transaction->get_transaction_id(), key, file_hdr_->col_tot_len_, value, disk_manager_->get_file_name(fd_));
    }
    return false;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）

    //1.1
    if(node->is_root_page() == true){
        return adjust_root(node);
    }

    //1.2
    //判断节点数是否合法
    int num_vals = node->get_num_vals();
    
    if(num_vals>= node->get_min_size() && num_vals< node->get_max_size()){
        //合法
        return false;
    }

    IxNodeHandle* parent_node = fetch_node(node->get_parent_page_no());
    IxNodeHandle* prev_node = get_prev_node(node);
    IxNodeHandle* next_node = get_next_node(node);
    node_guard parent_ng(parent_node, true);
    node_guard prev_ng(prev_node, true);
    node_guard next_ng(next_node, true);

    //判断重分配
    auto is_need_redistribute = [&](IxNodeHandle* check_node){
        return check_node->get_num_vals() + node->get_num_vals() >= node->get_min_size()*2;
    };

    //优先前驱结点
    if(prev_node!=nullptr && is_need_redistribute(prev_node) && node->get_parent_page_no() == prev_node->get_parent_page_no()){
        redistribute(prev_node, node, parent_node, 1);//是前驱结点
    }
    //互斥
    else if(next_node!=nullptr && is_need_redistribute(next_node) && node->get_parent_page_no() == next_node->get_parent_page_no()){
        redistribute(next_node, node, parent_node, 0);//不是前驱结点
    }    
    //互斥
    else{
        //TODO 考虑并发
        if(prev_node!=nullptr  && !is_need_redistribute(prev_node) && node->get_parent_page_no() == prev_node->get_parent_page_no()){
            coalesce(&prev_node, &node, &parent_node, 1, transaction, root_is_latched); //node在右边
        }
        else if(next_node!=nullptr && !is_need_redistribute(next_node) && node->get_parent_page_no() == next_node->get_parent_page_no()){
            coalesce(&next_node, &node, &parent_node, 0, transaction, root_is_latched); //node在左边
        }
        else{
            #ifdef DEBUG_IX_INDEX_HANDLE
                auto show_debug = this->get_bp_tree();
            #endif
            assert(0 && "Error No Operator Matched Node.");
        }
    }

    return true;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作

    if(!old_root_node->is_leaf_page() && old_root_node->page_hdr->num_key==0){// 内部结点的值比键多1
        //把它的孩子更新成新的根结点
        //取数据
        IxNodeHandle* child_node = fetch_node(old_root_node->get_rid(0)->page_no);
        node_guard child_ng(child_node, true);
        
        //维护file_hdr
        //first_leaf, last_leaf在下层维护
        //TODO: page_nums 不变, 与disk_manager适配
        file_hdr_->root_page_ = child_node->get_page_no();
        
        //维护child_node的元数据
        //其他的元数据均在下层维护
        child_node->page_hdr->parent = INVALID_PAGE_ID;
        //删除根的page
        buffer_pool_manager_->delete_page({this->fd_, old_root_node->get_page_no()});
        return true;
    }   

    else if(old_root_node->is_leaf_page() && old_root_node->page_hdr->num_key==0){// 叶结点的值和键相等
        //直接更新root page
        //啥也不做
        return false;
    }

    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论
    #ifdef DEBUG_IX_INDEX_HANDLE
        auto neighbor_keys_before = neighbor_node->get_all_keys();
        auto node_keys_before = node->get_all_keys();
        auto parent_keys_before = parent->get_all_keys();
        auto unused_before = 1;
    #endif
    const int MOVE_PAIR_NUMS = (neighbor_node->get_num_vals() + node->get_num_vals()) / 2 - node->get_num_vals();
    if(index == 0){
        //neighbor是node后继结点，表示：node(left)      neighbor(right)
        if(neighbor_node->is_leaf_page()){
            //获取右边结点的第一个键的位置
            int pos = parent -> lower_bound(neighbor_node->get_key(0));
            assert(pos>=0 && pos< parent->get_size());
            //把neighbor的前MOVE_PAIR_NUMS插入到node末尾中
            node->insert_pairs(node->get_size(), node->get_num_vals(),neighbor_node->get_key(0), neighbor_node->get_rid(0), MOVE_PAIR_NUMS);
            //删除neighbor的前MOVE_PAIR_NUMS个键
            for(int i = MOVE_PAIR_NUMS; i< neighbor_node->get_size(); i++){
                //将i swap 到 i - MOVE_PAIR_NUMS
                neighbor_node->key_swap(i, i - MOVE_PAIR_NUMS);
            }
            for(int i = MOVE_PAIR_NUMS; i<  neighbor_node->get_num_vals();i++){
                neighbor_node->rid_swap(i, i - MOVE_PAIR_NUMS);
            }
            //修改父亲的信息
            parent -> set_key(pos, neighbor_node->get_key(0));
        }
        else{
            //获得第一个比左边结点大的位置
            int pos = -1;
            //TODO: 封装为find_child函数.
            if(node->get_size()>0){
                pos = parent -> upper_bound(node->get_key(node->get_size() - 1));
            }
            else{
                for(int i = 0;i<parent->get_num_vals();i++){
                    if(parent->get_rid(i)->page_no == node->get_page_no()){
                        pos = i;
                        break;
                    }
                }
            }
            assert(pos>=0 && pos< parent->get_size());
            node->insert_pair(node->get_size(), node->get_num_vals(), parent->get_key(pos), *(neighbor_node->get_rid(0)));
            node->insert_pairs(node->get_size(), node->get_num_vals(), neighbor_node->get_key(0), neighbor_node->get_rid(1), MOVE_PAIR_NUMS - 1);
            
            //修改父亲的信息
            parent ->set_key(pos, neighbor_node->get_key(MOVE_PAIR_NUMS - 1));
            //删除neighbor的前MOVE_PAIR_NUMS个键
            for(int i = MOVE_PAIR_NUMS; i< neighbor_node->get_size(); i++){
                //将i swap 到 i - MOVE_PAIR_NUMS
                neighbor_node->key_swap(i, i - MOVE_PAIR_NUMS);
            }
            for(int i = MOVE_PAIR_NUMS; i<  neighbor_node->get_num_vals();i++){
                neighbor_node->rid_swap(i, i - MOVE_PAIR_NUMS);
            }

        }
        
        //维护两个结点的元数据(node已在insert维护)
        neighbor_node->page_hdr->num_key -= MOVE_PAIR_NUMS;


        //对于插入的数据 maintain_child
        //node的get_size已经改变
        for(int i = 0; i<MOVE_PAIR_NUMS;i++){
            maintain_child(node, node->get_num_vals()- 1 - i);
        }
    }
    else if(index > 0){
        //neighbor是node前驱结点，表示：neighbor(left)  node(right)

        //叶子节点
        if(node->is_leaf_page()){
            //获取右边结点的第一个键的位置
            int pos = parent -> lower_bound(node->get_key(0));
            assert(pos>=0 && pos< parent->get_size());
            //把neighbor的后MOVE_PAIR_NUMS插入到node头中
            node->insert_pairs(0, 0, neighbor_node->get_key(neighbor_node->get_size() - MOVE_PAIR_NUMS),neighbor_node->get_rid(neighbor_node->get_num_vals() - MOVE_PAIR_NUMS), MOVE_PAIR_NUMS);
            //删除neighbor的后MOVE_PAIR_NUMS个键
            //DONOTHING
            //修改父亲的信息
            parent -> set_key(pos, node->get_key(0));
        }
        else{
            int pos = parent -> upper_bound(neighbor_node->get_key(neighbor_node->get_size() - 1));
            assert(pos>=0 && pos< parent->get_size());
            for(int i = node->get_size() - 1;i>=0;i--){
                node->key_swap(i, i + MOVE_PAIR_NUMS);
            }
            for(int i = node->get_num_vals() - 1;i>=0;i--){
                node->rid_swap(i, i + MOVE_PAIR_NUMS);
            }
            node->set_key(MOVE_PAIR_NUMS-1, parent->get_key(pos));
            node->set_rid(MOVE_PAIR_NUMS-1, *(neighbor_node->get_rid(neighbor_node->get_num_vals() - 1)));

            //把neighbor的后面N-1个挪到node的前N-1个
            int begin_i = neighbor_node->get_size() - (MOVE_PAIR_NUMS - 1);
            for(int i = begin_i; i<neighbor_node->get_size();i++){
                node->set_key(i - begin_i, neighbor_node->get_key(i));
            }

            begin_i = neighbor_node->get_num_vals() - (MOVE_PAIR_NUMS);
            for(int i = begin_i; i<neighbor_node->get_num_vals()-1; i++){
                node->set_rid(i - begin_i, *(neighbor_node->get_rid(i)));
            }

            node->set_size(node->get_size() + MOVE_PAIR_NUMS);
            parent->set_key(pos, neighbor_node->get_key(neighbor_node->get_size() - MOVE_PAIR_NUMS));

        }

        //维护两个结点的元数据(node已在insert维护)
        neighbor_node->page_hdr->num_key -= MOVE_PAIR_NUMS;

        //对于插入的数据 maintain_child
        //node的get_size已经改变
        for(int i = 0; i<MOVE_PAIR_NUMS;i++){
            maintain_child(node, i);
        }
    }
    else{
        assert(0);
    }
    #ifdef DEBUG_IX_INDEX_HANDLE
        auto neighbor_keys_after = neighbor_node->get_all_keys();
        auto node_keys_after = node->get_all_keys();
        auto parent_keys_after = parent->get_all_keys();
        auto unused_after = 1;
    #endif
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf
    
    //如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node
    if(index==0){
        std::swap(node, neighbor_node);
    }   

    //TODO
    IxNodeHandle * parent_node = *parent;
    IxNodeHandle* left_node = *neighbor_node;
    IxNodeHandle* right_node = *node;
    #ifdef DEBUG_IX_INDEX_HANDLE
        auto left_keys_before = left_node->get_all_keys();
        auto parent_keys_before = parent_node->get_all_keys();
        auto right_keys_before = right_node->get_all_keys();
    #endif

    int pos = -1;
    if(left_node->get_size()>0){
        pos = parent_node->upper_bound(left_node->get_key(left_node->get_size() -1));
    }
    else{
        for(int i = 0;i<parent_node->get_num_vals(); i++){
            if(parent_node->get_rid(i)->page_no == left_node->get_page_no()){
                pos = i;
                break;
            }
        }
    }

    //把right插到left
    if(!right_node->is_leaf_page()){
        //中间结点
        left_node->insert_pair(left_node->get_size(), left_node->get_num_vals(), parent_node->get_key(pos), *(right_node->get_rid(0)));
        left_node->insert_pairs(left_node->get_size(), left_node->get_num_vals(), right_node->get_key(0), right_node->get_rid(1), right_node->get_size());
    }
    else{
        //叶节点
        left_node->insert_pairs(left_node->get_size(), left_node->get_num_vals(), right_node->get_key(0), right_node->get_rid(0), right_node->get_size());
    }

    //更新父节点
    parent_node->erase_pair(pos);

    //更新leaf的next和prev
    if(right_node->is_leaf_page()){
        if(right_node->get_page_no() == file_hdr_->last_leaf_){
            file_hdr_->last_leaf_ = left_node->get_page_no();
            left_node->set_next_leaf(INVALID_PAGE_ID);
        }
        else{
            left_node->set_next_leaf(right_node->get_next_leaf());
            IxNodeHandle* right_next_node = fetch_node(right_node->get_next_leaf());
            right_next_node->set_prev_leaf(left_node->get_page_no());
            node_guard rn_ng(right_next_node, true);
        }
    }

    for(int i = left_node->get_num_vals() - 1;i>=left_node->get_num_vals() - right_node->get_num_vals(); i--){
        maintain_child(left_node, i);
    }

    #ifdef DEBUG_IX_INDEX_HANDLE
        auto left_keys_after = left_node->get_all_keys();
        auto parent_keys_after = parent_node->get_all_keys();
    #endif

    //先删除右结点
    buffer_pool_manager_->delete_page({this->fd_, right_node->get_page_no()});
    coalesce_or_redistribute(parent_node, transaction, root_is_latched);

    return false;
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    node_guard ng(node, false);
    if (iid.slot_no >= node->get_size()) {
        throw IndexEntryNotFoundError();
    }
    Rid ret = *node->get_rid(iid.slot_no);
    //销毁node
    return ret;
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key, Context* context_) {
    auto leaf_page = find_leaf_page(key, Operation::FIND, context_->txn_, false);
    #ifdef ENABLE_LOCK_CRABBING
        lock_txn_guard ltg(context_->txn_, this);
    #endif
    #ifdef DEBUG_IX_INDEX_HANDLE
        auto gdb_debug_unused = leaf_page.first->get_all_keys();
    #endif
    node_guard leaf_ng(leaf_page.first, false);
    int target_slot_ = leaf_page.first->lower_bound(key);

    page_id_t page_no = leaf_page.first->get_page_id().page_no;
    if(target_slot_ == leaf_page.first->get_size()) {
        if(leaf_page.first->get_page_no()!=file_hdr_->last_leaf_){
            return Iid{leaf_page.first->get_next_leaf(), 0};
        }
        else{
            return Iid{page_no, leaf_page.first->get_size()};
        }
    }
    //将page_no传给上层, 上层得到page_no后，再用fetch_node获得node
    return Iid{page_no, target_slot_};
}

Iid IxIndexHandle::binary_search(const char *key, Context* context_) {
    auto leaf_page = find_leaf_page(key, Operation::FIND, context_ == nullptr?nullptr:context_->txn_, false);
    #ifdef ENABLE_LOCK_CRABBING
        lock_txn_guard ltg(context_ == nullptr?nullptr:context_->txn_, this);
    #endif
    node_guard leaf_ng(leaf_page.first, false);
    int target_slot_ = leaf_page.first->binary_search(key);
    if(target_slot_ == -1) {
        return Iid{-1,-1};
    }

    page_id_t page_no = leaf_page.first->get_page_id().page_no;
    //将page_no传给上层，unpin page，上层得到page_no后，再用fetch_node获得node
    return Iid{page_no, target_slot_};
}


/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key, Context* context_) {
    auto leaf_page = find_leaf_page(key, Operation::FIND, context_->txn_, false);
    #ifdef ENABLE_LOCK_CRABBING
        lock_txn_guard ltg(context_->txn_, this);
    #endif
    node_guard leaf_ng(leaf_page.first, false);
    int target_slot_ = leaf_page.first->upper_bound(key);
    page_id_t page_no = leaf_page.first->page->get_page_id().page_no;
    
    if(target_slot_ == leaf_page.first->get_size()) {
        if(leaf_page.first->get_page_no()!=file_hdr_->last_leaf_){
            return Iid{leaf_page.first->get_next_leaf(), 0};
        }
        else{
            return Iid{page_no, leaf_page.first->get_size()};
        }
    }
    //将page_no传给上层，上层得到page_no后，再用fetch_node获得node
    return Iid{leaf_page.first->page->get_page_id().page_no, target_slot_};
}

bool IxIndexHandle::is_key_exists(const char *key, Transaction *transaction){
    IxNodeHandle* leaf_node = find_leaf_page(key, Operation::FIND, transaction, false).first;
    #ifdef ENABLE_LOCK_CRABBING
        lock_txn_guard ltg(transaction, this);
    #endif
    node_guard ng(leaf_node, false);
    Rid *ret = new Rid();
    if(!leaf_node->leaf_lookup(key, &ret)){
        delete ret;
        return false;
    }
    delete ret;
    return true;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    node_guard ng(node, false);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

int IxIndexHandle::first_ind_key() const {
    IxNodeHandle *node = fetch_node(file_hdr_->first_leaf_);
    node_guard ng(node, false);
    assert(file_hdr_->col_types_[0] == ColType::TYPE_INT);
    if(node->get_size() == 0){
        return 1;
    }
    return *(int*)(node->get_key(0));
}


int IxIndexHandle::last_ind_key() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    node_guard ng(node, false);
    assert(file_hdr_->col_types_[0] == ColType::TYPE_INT);
    if(node->get_size() == 0){
        return 0;
    }
    return *(int*)(node->get_key(node->get_size() - 1));
}

bool IxIndexHandle::find_ind_key_at(const Iid& iid_, int& ret) const{
    if(iid_.page_no == -1){
        return false;
    }
    IxNodeHandle *node = fetch_node(iid_.page_no);
    node_guard ng(node, false);
    assert(file_hdr_->col_types_[0] == ColType::TYPE_INT);
    if(iid_.slot_no >= node->get_size()){
        return false;
    }
    ret = *(int*)(node->get_key(iid_.slot_no));
    return true;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page, this->buffer_pool_manager_);
    
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page, this->buffer_pool_manager_);
    return node;
}


void IxIndexHandle::maintain_parent(IxNodeHandle *node, const char* old_key, const char* new_key){
    IxNodeHandle* curr_node = node;
    const int& root_page = this->file_hdr_->root_page_;
    int tmp_page_no = curr_node->get_page_no();
    int tmp_parent_no = curr_node->get_parent_page_no();
    while(tmp_page_no!=root_page){
        curr_node = fetch_node(tmp_parent_no);
        tmp_page_no = tmp_parent_no;
        tmp_parent_no = curr_node->get_parent_page_no();
        node_guard ng(curr_node, false);
        int pos = curr_node->binary_search(old_key);
        if(pos!=-1){
            //找到了
            curr_node->set_key(pos, new_key);
            ng.is_dirty = true;
            return;
        }
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    node_guard prev_ng(prev, true);
    prev->set_next_leaf(leaf->get_next_leaf());

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    node_guard next_ng(next, true);
    next->set_prev_leaf(leaf->get_prev_leaf());  // 注意此处是SetPrevLeaf()
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        node_guard child_ng(child, true);
        child->set_parent_page_no(node->get_page_no());
    }
}