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

//�̱߳����ļ���
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
		//��ת��range����ʼλ��
		path.seekg(_range_start, std::ios::beg);
		std::string body;
		body.resize(_range_len);
		//��ȡrange�ֿ�����
		path.read(&body[0], _range_len);
		if (!path.good())
		{
			std::cerr << "read file " << _file << " range data failed\n";
			_res = false;
			return;
		}
		path.close();
		//�ϴ�range����
		bf::path name(_file);
		//��֯uri�ϴ�·��
		std::string uri = BACKUP_URI + name.filename().string();
		//ʵ����һ��httplib�ͻ��˶���---�ο�httplib�ļ�ʹ�ÿͻ���
		httplib::Client cli(SERVER_IP, SERVER_PORT);
		//����http����ͷ��Ϣ
		httplib::Headers hdr;
		hdr.insert(std::make_pair("Content-Length", std::to_string(_range_len)));
		std::stringstream tmp;
		tmp << "bytes=" << _range_start << "-" << (_range_start + _range_len - 1);
		hdr.insert(std::make_pair("Range", tmp.str().c_str()));
		//cli.Put(&uri[0],body,"text/plain");
		//�����˷���put����
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

//�Ʊ��ݿͻ�����
class CloudClient
{
private:
	std::unordered_map<std::string, std::string> backup_list;
private:
	bool GetBackupInfo() {
		//filename1 etag\n   �ļ��ָ�
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
		//��ӱ�����Ϣ
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
		//Ŀ¼���
		bf::path file(path);
		bf::directory_iterator item_begin(file);
		bf::directory_iterator item_end;
		for (; item_begin != item_end; ++item_begin)
		{
			if (bf::is_directory(item_begin->status()))
			{
				//�����Ŀ¼ҲҪɨ��������ļ�
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
			//�ϴ��ļ��ɹ�������ļ�etag��Ϣ
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
		//��ȡ�ļ���etag��Ϣ
		bf::path path(file);
		if (!bf::exists(path))
		{
			std::cerr << "get file " << file << " etag error";
			return false;
		}
		int64_t fsize = bf::file_size(path);//�ļ���С
		int64_t mtime = bf::last_write_time(path);//�ļ����һ���޸�ʱ��
		std::stringstream t;
		t << std::hex << fsize << "-" << std::hex << mtime;
		backup_list[file] = t.str();
		return true;
	}
	bool FileIsNeedBackup(const std::string &file){
		//�ж��ļ��Ƿ���Ҫ����
		std::string etag;
		if (GetFileEtag(file, etag) == false)
		{
			return false;
		}
		auto it = backup_list.find(file);
		if (it != backup_list.end() && it->second == etag)
		{
			//����Ҫ����
			return false;
		}
		return true;
	}
	bool PutFilData(const std::string &file) {
		//�����ļ�
		//���ֿ��С��10M�����ļ����ݽ��зֿ鴫��
		//ͨ����ȡ�ֿ鴫���Ƿ�ɹ��ж������ļ������Ƿ��ϴ��ɹ�
		//ѡ����̴߳���
		//1.��ȡ�ļ���С
		int64_t fsize = bf::file_size(file);
		if (fsize < 0)
		{
			std::cerr << "file " << file << " unnecessary backup";
			return false;
		}
		//2.�����ܹ���Ҫ�ֶ��ٿ飬�õ�ÿ���С�Լ���ʼλ��
		//3.ѭ�������̣߳����߳����ϴ��ļ�����
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
		//4.�ȴ������߳��˳����ж��ļ��ϴ����
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
		//5.�ϴ��ɹ���������ļ�������Ϣ��¼
		if (ret == false) {
			return false;
		}
		std::cerr << "file: [" << file << " ] backup success\n";
		//�����������һ���ֿ鴫��ʧ��֮���ȫ���ش�
		//���Ż���ĳ���ֿ鴫��ʧ��֮��Ӧ���ش��÷ֿ飬�����Ч��
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