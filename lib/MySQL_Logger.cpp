#include <fstream>
#include "proxysql.h"
#include "cpp.h"
#include <dirent.h>
#include <libgen.h>
#include "json_object.h"

static uint8_t mysql_encode_length(uint64_t len, unsigned char *hd) {
	if (len < 251) return 1;
	if (len < 65536) { if (hd) { *hd=0xfc; }; return 3; }
	if (len < 16777216) { if (hd) { *hd=0xfd; }; return 4; }
	if (hd) { *hd=0xfe; }
	return 9;
}

static inline int write_encoded_length(unsigned char *p, uint64_t val, uint8_t len, char prefix) {
	if (len==1) {
		*p=(char)val;
		return 1;
	}
	*p=prefix;
	p++;
	memcpy(p,&val,len-1);
	return len;
}

MySQL_Event::MySQL_Event (uint32_t _thread_id, char * _username, char * _schemaname , uint64_t _start_time , uint64_t _end_time , uint64_t _query_digest, char *_client, size_t _client_len) {
	thread_id=_thread_id;
	username=_username;
	schemaname=_schemaname;
	start_time=_start_time;
	end_time=_end_time;
	query_digest=_query_digest;
	client=_client;
	client_len=_client_len;
	et=PROXYSQL_QUERY;
	hid=UINT64_MAX;
	server=NULL;
}

void MySQL_Event::set_query(const char *ptr, int len) {
	query_ptr=(char *)ptr;
	query_len=len;
}

void MySQL_Event::set_server(int _hid, const char *ptr, int len) {
	server=(char *)ptr;
	server_len=len;
	hid=_hid;
}

uint64_t MySQL_Event::write(std::fstream *f, enum log_event_format event_format) {
	uint64_t total_bytes=0;
	switch (et) {
		case PROXYSQL_QUERY:
			total_bytes=write_query(f, event_format);
			break;
		default:
			break;
	}
	return total_bytes;
}

uint64_t MySQL_Event::write_query(std::fstream *f, enum log_event_format event_format) {
    if (event_format == BINARY) {
        return write_binary_query(f);
    } else if (event_format == JSON) {
        return write_json_query(f);
    } else {
        // Logging format is invalid or unspecified
        proxy_warning("Skipping logging, specified format is invalid");
        return 0;
    }
}

uint64_t MySQL_Event::write_binary_query(std::fstream *f) {
	uint64_t total_bytes=0;
	total_bytes+=1; // et
	total_bytes+=mysql_encode_length(thread_id, NULL);
	username_len=strlen(username);
	total_bytes+=mysql_encode_length(username_len,NULL)+username_len;
	schemaname_len=strlen(schemaname);
	total_bytes+=mysql_encode_length(schemaname_len,NULL)+schemaname_len;

	total_bytes+=mysql_encode_length(client_len,NULL)+client_len;

	total_bytes+=mysql_encode_length(hid, NULL);
	if (hid!=UINT64_MAX) {
		total_bytes+=mysql_encode_length(server_len,NULL)+server_len;
	}

	total_bytes+=mysql_encode_length(start_time,NULL);
	total_bytes+=mysql_encode_length(end_time,NULL);
	total_bytes+=mysql_encode_length(query_digest,NULL);

	total_bytes+=mysql_encode_length(query_len,NULL)+query_len;

	// write total length , fixed size
	f->write((const char *)&total_bytes,sizeof(uint64_t));
	//char prefix;
	uint8_t len;

	f->write((char *)&et,1);

	len=mysql_encode_length(thread_id,buf);
	write_encoded_length(buf,thread_id,len,buf[0]);
	f->write((char *)buf,len);

	len=mysql_encode_length(username_len,buf);
	write_encoded_length(buf,username_len,len,buf[0]);
	f->write((char *)buf,len);
	f->write(username,username_len);

	len=mysql_encode_length(schemaname_len,buf);
	write_encoded_length(buf,schemaname_len,len,buf[0]);
	f->write((char *)buf,len);
	f->write(schemaname,schemaname_len);

	len=mysql_encode_length(client_len,buf);
	write_encoded_length(buf,client_len,len,buf[0]);
	f->write((char *)buf,len);
	f->write(client,client_len);

	len=mysql_encode_length(hid,buf);
	write_encoded_length(buf,hid,len,buf[0]);
	f->write((char *)buf,len);

	if (hid!=UINT64_MAX) {
		len=mysql_encode_length(server_len,buf);
		write_encoded_length(buf,server_len,len,buf[0]);
		f->write((char *)buf,len);
		f->write(server,server_len);
	}

	len=mysql_encode_length(start_time,buf);
	write_encoded_length(buf,start_time,len,buf[0]);
	f->write((char *)buf,len);

	len=mysql_encode_length(end_time,buf);
	write_encoded_length(buf,end_time,len,buf[0]);
	f->write((char *)buf,len);

	len=mysql_encode_length(query_digest,buf);
	write_encoded_length(buf,query_digest,len,buf[0]);
	f->write((char *)buf,len);

	len=mysql_encode_length(query_len,buf);
	write_encoded_length(buf,query_len,len,buf[0]);
	f->write((char *)buf,len);
	if (query_len) {
		f->write(query_ptr,query_len);
	}

	return total_bytes;
}

// Constant JSON object key names for query log events.
const char* THREAD_ID_KEY="thread_id";
const char* USERNAME_KEY="username";
const char* SCHEMANAME_KEY="schemaname";
const char* START_TIME_KEY="start_time";
const char* END_TIME_KEY="end_time";
const char* QUERY_DIGEST_KEY="query_digest";
const char* QUERY_KEY="query";
const char* SERVER_KEY="server";
const char* CLIENT_KEY="client";
const char* ET_KEY="et";
const char* HID_KEY="hid";

// These option settings for json_object_object_add_ex() allow json-c to skip some unnecessary checks/strdup()
// invocations, which can yield significant savings when serializing. They require some care to be taken whenever
// new fields are added to the output format: 1) duplicate key names are not allowed and 2) key names must be constants.
const unsigned JSON_OBJ_ADD_OPTS=(JSON_C_OBJECT_ADD_KEY_IS_NEW | JSON_C_OBJECT_KEY_IS_CONSTANT);

uint64_t MySQL_Event::write_json_query(std::fstream *f) {
    json_object* event=json_object_new_object();

	size_t json_length = 0;
    if (json_object_object_add_ex(event, THREAD_ID_KEY, json_object_new_int64(thread_id), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, USERNAME_KEY, json_object_new_string(username), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, SCHEMANAME_KEY, json_object_new_string(schemaname), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, START_TIME_KEY, json_object_new_int64(start_time), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, END_TIME_KEY, json_object_new_int64(end_time), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, QUERY_DIGEST_KEY, json_object_new_int64(query_digest), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, QUERY_KEY, json_object_new_string_len(query_ptr, query_len), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, SERVER_KEY, json_object_new_string_len(server, server_len), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, CLIENT_KEY, json_object_new_string_len(client, client_len), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, ET_KEY, json_object_new_int64(PROXYSQL_QUERY), JSON_OBJ_ADD_OPTS) |
			json_object_object_add_ex(event, HID_KEY, json_object_new_int64(hid), JSON_OBJ_ADD_OPTS) != 0) {
    	// json-c doesn't tell us anything interesting about failures, so we can't include much else in the
    	// error message here.
    	proxy_error("Error building JSON");
    } else {
        // The char* returned by json_object_to_json_string_length() is owned by the json_object and will
        // be free'd when json_object_put() is called below.
		const char* json=json_object_to_json_string_length(event, JSON_C_TO_STRING_PLAIN, &json_length);
		(*f) << json << std::endl;
		json_length+=1; // the delimiter std::endl adds a byte
	}

    json_object_put(event);
    return json_length;
}

extern Query_Processor *GloQPro;

MySQL_Logger::MySQL_Logger() {
	enabled=false;
	event_format=BINARY; // BINARY is the default if unspecified
	base_filename=NULL;
	datadir=NULL;
	base_filename=strdup((char *)"");
#ifdef PROXYSQL_LOGGER_PTHREAD_MUTEX
	pthread_mutex_init(&wmutex,NULL);
#else
	spinlock_rwlock_init(&rwlock);
#endif
	logfile=NULL;
	log_file_id=0;
	max_log_file_size=100*1024*1024;
};

MySQL_Logger::~MySQL_Logger() {
	if (datadir) {
		free(datadir);
	}
	free(base_filename);
};

void MySQL_Logger::wrlock() {
#ifdef PROXYSQL_LOGGER_PTHREAD_MUTEX
	pthread_mutex_lock(&wmutex);
#else
  spin_wrlock(&rwlock);
#endif
};

void MySQL_Logger::wrunlock() {
#ifdef PROXYSQL_LOGGER_PTHREAD_MUTEX
	pthread_mutex_unlock(&wmutex);
#else
  spin_wrunlock(&rwlock);
#endif
};

void MySQL_Logger::flush_log() {
	if (enabled==false) return;
	wrlock();
	flush_log_unlocked();
	wrunlock();
}


void MySQL_Logger::close_log_unlocked() {
	if (logfile) {
		logfile->flush();
		logfile->close();
		delete logfile;
		logfile=NULL;
	}
}

void MySQL_Logger::flush_log_unlocked() {
	if (enabled==false) return;
	close_log_unlocked();
	open_log_unlocked();
}


void MySQL_Logger::open_log_unlocked() {
        log_file_id=find_next_id();
	if (log_file_id!=0) {
                log_file_id=find_next_id()+1;
	} else {
                log_file_id++;
	}
	char *filen=NULL;
	if (base_filename[0]=='/') { // absolute path
		filen=(char *)malloc(strlen(base_filename)+10);
		sprintf(filen,"%s.%08d",base_filename,log_file_id);
	} else { // relative path
		filen=(char *)malloc(strlen(datadir)+strlen(base_filename)+10);
		sprintf(filen,"%s/%s.%08d",datadir,base_filename,log_file_id);
	}
	logfile=new std::fstream();
	logfile->exceptions ( std::ofstream::failbit | std::ofstream::badbit );
	try {
		logfile->open(filen , std::ios::out | std::ios::binary);
		proxy_info("Starting new mysql log file %s\n", filen);
	}
	catch (std::ofstream::failure e) {
		proxy_error("Error creating new mysql log file %s\n", filen);
		delete logfile;
		logfile=NULL;
	}
	free(filen);
};

void MySQL_Logger::set_base_filename() {
	// if fileformat and filename are the same, return
	wrlock();
	max_log_file_size=mysql_thread___eventslog_filesize;
	if ((event_format == mysql_thread___eventslog_fileformat) && strcmp(base_filename,mysql_thread___eventslog_filename)==0) {
		wrunlock();
		return;
	}
	// close current log
	close_log_unlocked();
	// set file id to 0 , so that find_next_id() will be called
	log_file_id=0;
	free(base_filename);
	event_format=mysql_thread___eventslog_fileformat;
	base_filename=strdup(mysql_thread___eventslog_filename);
	if (strlen(base_filename)) {
		enabled=true;
		open_log_unlocked();
	} else {
		enabled=false;
	}
	wrunlock();
}

void MySQL_Logger::set_datadir(char *s) {
	datadir=strdup(s);
	flush_log();
};

void MySQL_Logger::log_request(MySQL_Session *sess, MySQL_Data_Stream *myds) {
	if (enabled==false) return;
	if (logfile==NULL) return;

	MySQL_Connection_userinfo *ui=sess->client_myds->myconn->userinfo;

	uint64_t curtime_real=realtime_time();
	uint64_t curtime_mono=sess->thread->curtime;
	int cl=0;
	char *ca=(char *)""; // default
	if (sess->client_myds->addr.addr) {
		ca=sess->client_myds->addr.addr;
	}
	cl+=strlen(ca);
	if (cl && sess->client_myds->addr.port) {
		ca=(char *)malloc(cl+8);
		sprintf(ca,"%s:%d",sess->client_myds->addr.addr,sess->client_myds->addr.port);
	}
	cl=strlen(ca);
	MySQL_Event me(sess->thread_session_id,ui->username,ui->schemaname,
		sess->CurrentQuery.start_time + curtime_real - curtime_mono,
		sess->CurrentQuery.end_time + curtime_real - curtime_mono,
		GloQPro->get_digest(&sess->CurrentQuery.QueryParserArgs),
		ca, cl
	);
	char *c=(char *)sess->CurrentQuery.QueryPointer;
	if (c) {
		me.set_query(c,sess->CurrentQuery.QueryLength);
	} else {
		me.set_query("",0);
	}

	int sl=0;
	char *sa=(char *)""; // default
	if (myds) {
		if (myds->myconn) {
			sa=myds->myconn->parent->address;
		}
	}
	sl+=strlen(sa);
	if (sl && myds->myconn->parent->port) {
		sa=(char *)malloc(sl+8);
		sprintf(sa,"%s:%d", myds->myconn->parent->address, myds->myconn->parent->port);
	}
	sl=strlen(sa);
	if (sl) {
		int hid=-1;
		hid=myds->myconn->parent->myhgc->hid;
		me.set_server(hid,sa,sl);
	}

	wrlock();

	me.write(logfile, event_format);


	unsigned long curpos=logfile->tellp();
	if (curpos > max_log_file_size) {
		flush_log_unlocked();
	}
	wrunlock();

	if (cl && sess->client_myds->addr.port) {
		free(ca);
	}
	if (sl && myds->myconn->parent->port) {
		free(sa);
	}
}

void MySQL_Logger::flush() {
	wrlock();
	if (logfile) {
		logfile->flush();
	}
	wrunlock();
}

unsigned int MySQL_Logger::find_next_id() {
	int maxidx=0;
	DIR *dir;
	struct dirent *ent;
	char *eval_filename = NULL;
	char *eval_dirname = NULL;
        char *eval_pathname = NULL;
	assert(base_filename);
	if (base_filename[0] == '/') {
		eval_pathname = strdup(base_filename);
		eval_filename = basename(eval_pathname);
		eval_dirname = dirname(eval_pathname);
	} else {
                assert(datadir);
		eval_filename = strdup(base_filename);
		eval_dirname = strdup(datadir);
	}
	size_t efl=strlen(eval_filename);
	if ((dir = opendir(eval_dirname)) != NULL) {
		while ((ent = readdir (dir)) != NULL) {
			if (strlen(ent->d_name)==efl+9) {
				if (strncmp(ent->d_name,eval_filename,efl)==0) {
					if (ent->d_name[efl]=='.') {
						int idx=atoi(ent->d_name+efl+1);
						if (idx>maxidx) maxidx=idx;
					}
				}
			}
		}
		closedir (dir);
		if (base_filename[0] != '/') {
			free(eval_dirname);
			free(eval_filename);
		}
                if (eval_pathname) {
                        free(eval_pathname);
                }
		return maxidx;
	} else {
        /* could not open directory */
		proxy_error("Unable to open datadir: %s\n", eval_dirname);
		exit(EXIT_FAILURE);
	}        
	return 0;
}
