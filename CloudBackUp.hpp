#ifndef __M_CLOUD_H_
#define __M_CLOUD_H_

#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>
#include <string>
#include <unordered_map>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include "httplib.h"

#define CLIENT_BACKUP_DIR "backup"
#define CLIENT_BACKUP_INFO_FILE "back.list"
#define RANGE_MAX_SIZE (10<<20)
#define SERVER_IP  "192.168.71.129"
#define SERVER_PORT 9000
#define BACKUP_URI "/list/"

namespace bf = boost::filesystem;

//线程备份文件类
class ThrBackUp {
private:
	std::string _file;
	int64_t _range_start;
	int64_t _range_len;
public:
	bool _res;
public:
	ThrBackUp(const std::string &file ,int64_t start,int64_t len) 
		:_res(true),_file(file),_range_start(start),_range_len(len)
	{}
	void start() {
		std::ifstream path(_file, std::ios::binary);
		if (!path.is_open())
		{
			std::cerr << "range backup file " << _file << " failed\n";
			_res = false;
			return;
		}
		//跳转到range的起始位置
		path.seekg(_range_start, std::ios::beg);
		std::string body;
		body.resize(_range_len);
		//读取range分块数据
		path.read(&body[0], _range_len);
		if (!path.good())
		{
			std::cerr << "read file " << _file << " range data failed\n";
			_res = false;
			return;
		}
		path.close();
		//上传range数据
		bf::path name(_file);
		//组织uri上传路径
		std::string uri = BACKUP_URI + name.filename().string();
		//实例化一个httplib客户端对象---参考httplib文件使用客户端
		httplib::Client cli(SERVER_IP, SERVER_PORT);
		//定义http请求头信息
		httplib::Headers hdr;
		hdr.insert(std::make_pair("Content-Length", std::to_string(_range_len)));
		std::stringstream tmp;
		tmp << "bytes=" << _range_start << "-" << (_range_start + _range_len - 1);
		hdr.insert(std::make_pair("Range", tmp.str().c_str()));
		//cli.Put(&uri[0],body,"text/plain");
		//向服务端发送put请求
		auto rsp = cli.Put(uri.c_str(), hdr, body, "text/plain");
		if (rsp&&rsp->status != 200)
		{
			_res = false;
		}
		std::stringstream ss;
		ss << "backup file " << _file << " range:[" << _range_start << "-" << _range_len << "] backup successs\n";
		std::cout << ss.str();
		return;
	}
};

//云备份客户端类
class CloudClient
{
private:
	std::unordered_map<std::string, std::string> backup_list;
private:
	bool GetBackupInfo() {
		//filename1 etag\n   文件分割
		//filename2 etag\n
		bf::path path(CLIENT_BACKUP_INFO_FILE);
		if (!bf::exists(path))
		{
			std::cerr << "list file " << path.string() << " is not exists\n";
			return false;
		}
		int64_t fsize = bf::file_size(path);
		if (fsize == 0)
		{
			std::cerr << "have no backup info\n";
			return false;
		}
		std::string body;
		body.resize(fsize);
		std::ifstream file(CLIENT_BACKUP_INFO_FILE, std::ios::binary);
		if (!file.is_open())
		{
			std::cerr << "list file open error\n";
			return false;
		}
		file.read(&body[0], fsize);
		if (!file.good())
		{
			std::cerr << "read list file body error\n";
			return false;
		}
		std::vector<std::string> list;
		boost::split(list, body, boost::is_any_of("\n"));
		for (auto e : list)
		{
			//filename etag\n
			size_t pos = e.find(" ");
			if (pos == std::string::npos)
			{
				continue;
			}
			std::string key = e.substr(0, pos);
			std::string val = e.substr(pos + 1);
			backup_list[key] = val;
		}
		return true;
	}
	bool SetBackUpInfo() {
		//添加备份信息
		std::string body;
		for (auto e : backup_list)
		{
			body += e.first + " " + e.second + "\n";
		}
		std::ofstream file(CLIENT_BACKUP_INFO_FILE, std::ios::binary);
		if (!file.is_open())
		{
			std::cerr << "open list file error\n";
			return false;
		}
		file.write(&body[0], body.size());
		if (!file.good())
		{
			std::cerr << "set backup info error\n";
			return false;
		}
		file.close();
		return true;
	}
	bool BackUpDirLiten(const std::string &path) {
		//目录监控
		bf::path file(path);
		bf::directory_iterator item_begin(file);
		bf::directory_iterator item_end;
		for (; item_begin != item_end; ++item_begin)
		{
			if (bf::is_directory(item_begin->status()))
			{
				//如果是目录也要扫描里面的文件
				BackUpDirLiten(item_begin->path().string());
				continue;
			}
			if (FileIsNeedBackup(item_begin->path().string()) == false)
			{
				continue;
			}
			if (PutFilData(item_begin->path().string()) == false)
			{
				continue;
			}
			//上传文件成功就添加文件etag信息
			AddBackupInfo(item_begin->path().string());
		}
		return true;
	}
	bool AddBackupInfo(const std::string &file) {
		std::string etag;
		if (GetFileEtag(file, etag) == false)
		{
			return false;
		}
		backup_list[file] = etag;
	}
	bool GetFileEtag(const std::string &file, std::string &etag) {
		//获取文件的etag信息
		bf::path path(file);
		if (!bf::exists(path))
		{
			std::cerr << "get file " << file << " etag error";
			return false;
		}
		int64_t fsize = bf::file_size(path);//文件大小
		int64_t mtime = bf::last_write_time(path);//文件最后一次修改时间
		std::stringstream t;
		t << std::hex << fsize << "-" << std::hex << mtime;
		backup_list[file] = t.str();
		return true;
	}
	bool FileIsNeedBackup(const std::string &file){
		//判断文件是否需要备份
		std::string etag;
		if (GetFileEtag(file, etag) == false)
		{
			return false;
		}
		auto it = backup_list.find(file);
		if (it != backup_list.end() && it->second == etag)
		{
			//不需要备份
			return false;
		}
		return true;
	}
	bool PutFilData(const std::string &file) {
		//备份文件
		//按分块大小（10M）对文件内容进行分块传输
		//通过获取分块传输是否成功判断整个文件内容是否上传成功
		//选择多线程处理
		//1.获取文件大小
		int64_t fsize = bf::file_size(file);
		if (fsize < 0)
		{
			std::cerr << "file " << file << " unnecessary backup";
			return false;
		}
		//2.计算总共需要分多少块，得到每块大小以及起始位置
		//3.循环创建线程，在线程中上传文件数据
		int count = (int)(fsize / RANGE_MAX_SIZE);
		std::vector<ThrBackUp> thr_res;
		std::vector<std::thread> thr_list;
		std::cerr << "file:[ " << file << " ] fsize:" << fsize << "  count: [" << count<<"]--" ;
		for (int i = 0; i <= count; ++i)
		{
			int64_t range_start = i* RANGE_MAX_SIZE;
			int64_t range_end = (i + 1)*RANGE_MAX_SIZE-1;
			if (i == count)
			{
				range_end = fsize-1;
				std::cout << range_end<<std::endl;
			}
			int64_t range_len = range_end - range_start + 1;
			ThrBackUp backup_info(file, range_start, range_len);
			std::cerr << "file :[" << file << "] range:[" << range_start << "-" << range_end << "] range_len:" << range_len << "\n";
			thr_res.push_back(backup_info);
		}
		for (int i = 0; i <= count; ++i)
		{
			thr_list.push_back(std::thread(thr_start, &thr_res[i]));
		}
		//4.等待所有线程退出，判断文件上传结果
		bool ret = true;
		for (int i = 0; i <= count; i++)
		{
			thr_list[i].join();
			if (thr_res[i]._res == true)
			{
				continue;
			}
			ret = false;
		}
		//5.上传成功，就添加文件备份信息记录
		if (ret == false) {
			return false;
		}
		std::cerr << "file: [" << file << " ] backup success\n";
		//我们这个程序一个分块传输失败之后就全部重传
		//（优化）某个分块传输失败之后应该重传该分块，会提高效率
		return true;
	}
	static void thr_start(ThrBackUp* backup_info) {
		backup_info->start();
		return;
	}

public:
	CloudClient() {
		bf::path file(CLIENT_BACKUP_DIR);
		if (!bf::exists(file))
		{
			bf::create_directory(file);
		}
	}
	bool Start(){
		GetBackupInfo();
		while (1) {
			BackUpDirLiten(CLIENT_BACKUP_DIR);
			SetBackUpInfo();
			Sleep(30000);
		}
		return true;
	}
};

#endif