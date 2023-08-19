// by Zealot
// MIT licence https://opensource.org/licenses/MIT

#define CURRENT_VERSION "4.4.3.0"

#include <cstring>
#include <cstdio>
#include <string>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <map>
#include <unordered_map>
#include <functional>
#include <memory>
#include <locale>
#include <codecvt>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <regex>
#include <queue>
#include <tuple>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <condition_variable>

#ifdef _WIN32
#include <Windows.h>
#include <direct.h>
#include <process.h>
#else
#include <dlfcn.h>
#endif

#include <curl/curl.h>
#include <zlib.h>

#include "easylogging++.h"
#include "json.hpp"
#include "OcapReplaySaver2.h"

#define REPLAY_FILEMASK "%Y_%m_%d__%H_%M_"

#define CMD_NEW_UNIT		":NEW:UNIT:"  // новый юнит зарегистрирована ocap
#define CMD_NEW_VEH			":NEW:VEH:"  // новая техника зарегистрирована ocap
#define CMD_EVENT			":EVENT:" // новое событие, кто-то зашел, вышел, стрельнул и попал
#define CMD_CLEAR			":CLEAR:" // очистить json
#define CMD_UPDATE_UNIT		":UPDATE:UNIT:" // обновить данные по позиции юнита
#define CMD_UPDATE_VEH		":UPDATE:VEH:" // обновить данные по позиции техники
#define CMD_SAVE			":SAVE:" // записать json во временный файл и попробовать отправить по сети
#define CMD_LOG				":LOG:" // сделать запись в отдельный файл лога
#define CMD_START			":START:" // начать запись ocap реплея
#define CMD_TIME			":TIME:" // initiate time when recording starts
#define CMD_FIRED			":FIRED:" // кто-то стрельнул
#define CMD_VERSION         ":VERSION:" // returns extension version
#define CMD_SET_VERSION     ":SET:VERSION:" // sets ocap addon version

#define CMD_MARKER_CREATE	":MARKER:CREATE:" // был создан маркер на карте
#define CMD_MARKER_DELETE	":MARKER:DELETE:" // маркер удалили
#define CMD_MARKER_MOVE		":MARKER:MOVE:" // маркер передвинули

#define ERROR_THROW(S) {LOG(ERROR) << S;throw ocapException(S); }
#define COMMAND_CHECK_INPUT_PARAMETERS(N) if(args.size()!=N){LOG(WARNING) << "Expected " << N << "arguments";ERROR_THROW("Unexpected number of given arguments!");}
#define COMMAND_CHECK_INPUT_PARAMETERS_N(N,M) if(args.size()<N || args.size()>M){LOG(WARNING) << "Expected between " << N << " and " << M << " arguments";ERROR_THROW("Unexpected number of given arguments!"); }
#define COMMAND_CHECK_WRITING_STATE	if(!is_writing.load()) {ERROR_THROW("Is not writing state!")}

#define JSON_STR_FROM_ARG(N) (json::string_t(filterSqfString(args[N])))
#define JSON_INT_FROM_ARG(N) (json::number_integer_t(stoi(args[N])))
#define JSON_FLOAT_FROM_ARG(N) (json::number_float_t(stod(args[N])))
#define JSON_PARSE_FROM_ARG(N) (json::parse(escapeArma3ToJson(args[N])))

#define DUMP_ARGS_TO_LOG if(true){stringstream ss;ss<<args.size()<<"[";for(int i=0;i<args.size();i++){if(i>0)ss<<"::";ss<<args[i];}ss<<"]";LOG(WARNING)<<"ARG DUMP:"<<ss.str();}


using namespace std;

void commandEvent(const vector<string>& args);
void commandNewUnit(const vector<string>& args);
void commandNewVeh(const vector<string>& args);
void commandSave(const vector<string>& args);
void commandUpdateUnit(const vector<string>& args);
void commandUpdateVeh(const vector<string>& args);
void commandClear(const vector<string>& args);
void commandLog(const vector<string>& args);
void commandStart(const vector<string>& args);
void commandTime(const vector<string>& args);
void commandFired(const vector<string>& args);

void commandMarkerCreate(const vector<string>& args);
void commandMarkerDelete(const vector<string>& args);
void commandMarkerMove(const vector<string>& args);
void commandSetVersion(const vector<string>& args);



namespace {
    using json = nlohmann::json;
    namespace fs = std::filesystem;
    using std::string;

    thread command_thread;
    queue<tuple<string, vector<string> > > commands;
    mutex command_mutex;
    condition_variable command_cond;
    atomic<bool> command_thread_shutdown(false);
    json j;
    bool curl_init = false;
    atomic<bool> is_writing(false);
    string mission_type;
    string addon_version;

    std::unordered_map<std::string, std::function<void(const vector<string>&)> > dll_commands = {
        { CMD_NEW_VEH,			commandNewVeh },
        { CMD_NEW_UNIT,			commandNewUnit },
        { CMD_CLEAR,			commandClear },
        { CMD_EVENT,			commandEvent },
        { CMD_UPDATE_VEH,		commandUpdateVeh },
        { CMD_UPDATE_UNIT,		commandUpdateUnit },
        { CMD_SAVE,				commandSave },
        { CMD_LOG,				commandLog },
        { CMD_START,			commandStart },
        { CMD_TIME, 			commandTime },
        { CMD_FIRED,			commandFired },
        { CMD_MARKER_CREATE,	commandMarkerCreate },
        { CMD_MARKER_DELETE,	commandMarkerDelete },
        { CMD_MARKER_MOVE,		commandMarkerMove   },
        { CMD_SET_VERSION,		commandSetVersion   }
    };

    class ocapException : public std::exception {
    public:
        ocapException() {};
        ocapException(const char* e) {};
    };

    struct {
        std::string newUrl{ "https://127.0.0.1/api/v1/operations/add" };
        std::string newServerGameType{ "TvT" };
        std::string newUrlRequestSecret{ "pwd1234" };
        int httpRequestTimeout{ 120 };
        int traceLog{ 0 };
        std::string logsDir{ "./OCAPLOG" };
        std::string tempDir{ "./OCAPTMP" };
        std::string logAndTmpPrefix{ "ocap-" };
        // TODO: new names and comments, logs dir, change types accordingly, chenage default values
    } config;
}

bool checkUTF8Bytes(std::string& in) {
    int pattern_length = 0;
    const char* in_cstr = in.c_str();
    const char* c_ptr = in_cstr;
    for (; *c_ptr != '\0'; ++c_ptr) {
        char in_char = *c_ptr;
        if (!pattern_length) {
            if (in_char >= '\0' && in_char <= '\x7F') // 1-byte sequence
                continue;
            if (in_char >= '\xC0' && in_char <= '\xDF') { // 2-byte sequence
                pattern_length = 1;
                continue;
            }
            if (in_char >= '\xE0' && in_char <= '\xEF') { // 3-byte sequence
                pattern_length = 2;
                continue;
            }
            if (in_char >= '\xF0' && in_char <= '\xF7') { // 4-byte sequence
                pattern_length = 3;
                continue;
            }
            if (in_char >= '\xF8' && in_char <= '\xFB') { // 5-byte sequence
                pattern_length = 4;
                continue;
            }
            if (in_char >= '\xFC' && in_char <= '\xFD') { // 6-byte sequence
                pattern_length = 5;
                continue;
            }
            // error, incorrect symbol
            goto err;
        }
        else {
            if (in_char >= '\x80' && in_char <= '\xBF') {
                --pattern_length;
                continue;
            }
            // error, incorrect symbol
            goto err;
        }
    }
    return false;
err:
    std::ostringstream ss;
    ss << "Unexpected symbol #" << c_ptr - in_cstr << ":" << std::hex << std::uppercase << std::setw(2) << std::setfill('0') << (int)((unsigned char)*c_ptr);
    ss << " in text:" << in << ":";
    for (unsigned char c : in) ss << std::hex << std::uppercase << std::setw(2) << std::setfill('0') << static_cast<int>(c) << " ";
    LOG(WARNING) << ss.str();
    return true;
}

bool write_compressed_data(const char* filename, const char* buffer, size_t buff_len) {
    bool result = true;
    gzFile gz = gzopen(filename, "wb");
    if (!gz) {
        LOG(ERROR) << "Cannot create file: " << filename;
        return false;
    }
    if (!gzwrite(gz, buffer, static_cast<unsigned int>(buff_len))) {
        LOG(ERROR) << "Error while file writing " << filename;
        result = false;
    }
    gzclose(gz);
    return result;
}

void perform_command(tuple<string, vector<string> >& command) {
    string function(std::move(std::get<0>(command)));
    vector<string> args(std::move(std::get<1>(command)));
    bool error = false;

    for (auto& str : args) {
        error = checkUTF8Bytes(str);
        if (error) break;
    }

    if (config.traceLog) {
        stringstream ss;
        ss << function << " " << args.size() << ":[";
        for (int i = 0; i < args.size(); i++) {
            if (i > 0)
                ss << "::";
            ss << args[i];
        }
        ss << "]";
        LOG(TRACE) << ss.str();
    }

    try {
        auto fn = dll_commands.find(function);
        if (fn == dll_commands.end()) {
            LOG(ERROR) << "Function is not supported! " << function;
        }
        else {
            fn->second(args);

        }
    }
    catch (const exception& e) {
        error = true;
        LOG(ERROR) << "Exception: " << e.what();
    }
    catch (...) {
        error = true;
        LOG(ERROR) << "Exception: Unknown";
    }

    if (error) {
        stringstream ss;
        ss << "Return, parameters were: " << function << " ";
        ss << args.size() << ":[";
        for (int i = 0; i < args.size(); i++) {
            if (i > 0)
                ss << "::";
            ss << args[i];
        }
        ss << "]";
        LOG(ERROR) << ss.str();
    }

}


void command_loop() {
    try {
        while (true) {
            unique_lock<mutex> lock(command_mutex);
            command_cond.wait(lock, [] {return !commands.empty() || command_thread_shutdown; });
            lock.unlock();

            while (true) {
                unique_lock<mutex> lock2(command_mutex);
                if (commands.empty() || command_thread_shutdown.load())
                {
                    break;
                }
                tuple<string, vector<string> > cur_command = std::move(commands.front());
                commands.pop();
                lock2.unlock();
                perform_command(cur_command);
            }
            if (command_thread_shutdown.load())
            {
                LOG(INFO) << "Exit flag is set. Quiting command loop.";
                return;
            }
        }
    }
    catch (const exception& e) {
        LOG(ERROR) << "Exception: " << e.what();
    }
    catch (...) {
        LOG(ERROR) << "Exception: Unknown";
    }
}

string removeHash(const string& c) {
    std::string r(c);
    r.erase(remove(r.begin(), r.end(), '#'), r.end());
    return r;
}


// escapes arma3 parameter string to JSON, "" to \", \ to \\ and all control characters to appropriate \u00xx sequences
std::string escapeArma3ToJson(const std::string& in) {
    std::string out;
    out.reserve(in.size() * 2);
    bool instr = false;
    for (auto it = in.cbegin(); it != in.cend(); ++it) {
        char in_char = *it;
        std::string app_str;
        app_str = in_char;

        if (instr && in_char == '\"') {
            if (std::next(it) != in.cend() && *std::next(it) == '\"') {
                app_str = "\\\""; it++;
            }
            else {
                instr = false;
            }
        }

        if (!instr && in_char == '\"')
            instr = true;

        if (in_char >= '\u0000' && in_char <= '\u001f')
        {
            switch (in_char)
            {
            case '\r':
                app_str = "\\r"; break;
            case '\b':
                app_str = "\\b"; break;
            case '\n':
                app_str = "\\n"; break;
            case '\t':
                app_str = "\\t"; break;
            case '\f':
                app_str = "\\f"; break;
            default:
                char buff[16];
                snprintf(buff, 16, "\\u00%02x", in_char);
                app_str = buff;
                break;
            }
        }
        if (in_char == '\\') {
            app_str = "\\\\";
        }
        out += app_str;
    }
    return out;
}

// убирает начальные и конечные кавычки в текстк, сдвоенные кавычки превращает в одинарные
// removes beginning and ending quote signs ('") in text, double quotes "" makes ordinary "
// sqf str command only escapes double quote signs and add " in the begin and end of value
// this command perform reverse operation
void filterSqfStr(const char* s, char* r) {
    bool begin = true;
    while (*s) {
        if (begin && *s == '"' ||
            *(s + 1) == '\0' && *s == '"')
            goto nxt;

        if (*(s + 1) == '"' && *s == '"') {
            *r = '"'; ++r; ++s;
            goto nxt;
        }
        *r = *s; ++r;
    nxt:
        ++s;
        begin = false;
    }
    *r = '\0';
}

string filterSqfString(const string& Str) {
    unique_ptr<char[]> out(new char[Str.size() + 1]);
    filterSqfStr(Str.c_str(), static_cast<char*>(out.get()));
    return string(static_cast<char*>(out.get()));
}


void prepareMarkerFrames(int frames) {
    LOG(TRACE) << frames;

    try {
        if (j["Markers"].is_null() || !j["Markers"].is_array() || j["Markers"].size() < 1) {
            LOG(ERROR) << j["Markers"] << "has incorrect state!";
            return;
        }

        /*
        marker saves as:
        [
        0	:	“marker type”, // “” marker icon name
        1	:	“marker text”,
        2	:	12, //who created marker, ID
        3	:	“000000”, //цвет в HEX
        4	:	52, // start frame
        5	:	104, // end frame
        6	:	0, // 0 -east, 1 - west, 2 - resistance, 3- civilian, -1 - global
        7	:	[[52,[1000,5000],180],[70,[1500,5600],270]] // positions
        8   : [1,1] // markerSize, NEW
        9   : "ICON" // markerShape NEW
        ]
        */
        /*
        [0:_mname , 1: 0, 2 : swt_cfgMarkers_names select _type, 3: _mtext, 4: ocap_captureFrameNo, 5:-1, 6: _pl getVariable ["ocap_id", 0],
        7: call bis_fnc_colorRGBtoHTML, 8:[1,1], 9:side _pl call BIS_fnc_sideID, 10:_mpos]]
        оставить:  2, 3, 4, 5, 6, 7, 9, 10
        */

        // cleans "Markers"

        for (auto& ja : j["Markers"]) {
            if (ja[3].get<int>() == -1) {
                ja[3] = frames;
            }

            ja.erase(ja.cbegin() + 9); // remove markerId, as it could be long
            //j["Markers"][i] = json::array({ja[2], ja[3], ja[4], ja[5], ja[6], ja[7],ja[9], ja[10]});
        }
    }
    catch (exception& e) {
        LOG(ERROR) << "Error happens" << e.what();
        return;

    }
    catch (...)
    {
        LOG(ERROR) << "Error happens" << frames;
        return;
    }

}

fs::path getAndCreateLogDirectory() {
    fs::path res = config.logsDir;
    //res += "/";
    //res += "OCAPLOG";
    res.make_preferred();
    try {
        fs::create_directories(res);
    }
    catch (const std::exception& ex) {
        LOG(WARNING) << "Error create directories: " << ex.what();
    }
    return res;
}

fs::path getAndCreateTempDirectory() {
    fs::path res = config.tempDir;
    //res += "/";
    //res += "OCAPLOG";
    res.make_preferred();
    try {
        fs::create_directories(res);
    }
    catch (const std::exception& ex) {
        LOG(WARNING) << "Error create directories: " << ex.what();
    }
    return res;
}

std::string uniqueFileName() {
    std::srand(std::time(nullptr));
    std::string res(5, '\0');
    std::generate_n(res.begin(), 5, []() {
        const char chars[] = "0123456789abcdefghijklmnopqrstuvwxyz";
        constexpr size_t m_index = (sizeof(chars) - 1);
        return chars[rand() % m_index];
    });
    return res;
}

pair<std::string, std::optional<std::string> > saveCurrentReplayToTempFile() {
    fs::path temp_fname = getAndCreateTempDirectory();
    temp_fname += "/";

    temp_fname += config.logAndTmpPrefix;
    temp_fname += uniqueFileName();
    temp_fname.make_preferred();

    LOG(TRACE) << "Temp file name:" << temp_fname;

    fstream currentReplay(temp_fname, fstream::out | fstream::binary);
    if (!currentReplay.is_open()) {
        LOG(ERROR) << "Cannot open result file: " << temp_fname;
        throw ocapException("Cannot open temp file!");
    }

    string all_replay;

    try {
        if (config.traceLog)
            all_replay = j.dump(2, ' ', false, json::error_handler_t::ignore);
        else
            all_replay = j.dump(-1, ' ', false, json::error_handler_t::ignore);
    }
    catch (exception& e) {
        LOG(ERROR) << "Error happens while JSON dumping:" << e.what();
        throw;
    }

    currentReplay << all_replay;
    currentReplay.flush();
    currentReplay.close();

    vector<uint8_t> archive;
    LOG(INFO) << "Replay saved:" << temp_fname;
    std::optional<std::string> archive_name;

    if (true) {
        archive_name = std::make_optional<std::string>(temp_fname.string() + ".gz");
        if (write_compressed_data(archive_name->c_str(), all_replay.c_str(), all_replay.size())) {
            LOG(INFO) << "Archive saved:" << *archive_name << " Removing uncompressed file.";
            remove(temp_fname);
        }
        else {
            LOG(WARNING) << "Archive not saved! " << *archive_name;
            archive_name.reset();
        }
    }

    return make_pair(temp_fname.string(), archive_name);
}

std::string& utf8to_translit(const std::string& in, std::string& out) {
    static const char* symbols[][2] = {
        {"\xc3\x91", "N"}, /* Ñ */  {"\xc3\xb1", "n"}, /* ñ */ {"\xc3\x87", "C"}, /* Ç */ {"\xc3\xa7", "c"}, /* ç */ {"\xe2\x80\x94", ""}, /* MDASH */
        {"\xe2\x80\x93", "-"}, /* – NDASH */ {"\xe2\x80\x9c", ""}, /* “ */ {"\xe2\x80\x9d", ""}, /* ” */ {"\xc2\xab", ""}, /* « */ {"\xc2\xbb", ""}, /* » */
        {"\xe2\x80\x9e", ""}, /* „ */ {"\xe2\x80\x99", ""}, /* ’ */ {"\xe2\x80\x98", ""}, /* ‘ */ {"\xe2\x80\xb9", ""}, /* ‹ */ {"\xe2\x80\xba", ""}, /* › */
        {"\xe2\x80\x9a", ""}, /* ‚ */ {"\xc3\x9a", "U"}, /* Ú */ {"\xc3\x99", "U"}, /* Ù */ {"\xc3\x9b", "U"}, /* Û */ {"\xc3\x9c", "U"}, /* Ü */
        {"\xc3\xba", "u"}, /* ú */ {"\xc3\xb9", "u"}, /* ù */ {"\xc3\xbb", "u"}, /* û */ {"\xc3\xbc", "u"}, /* ü */ {"\xc3\x93", "O"}, /* Ó */
        {"\xc3\x92", "O"}, /* Ò */ {"\xc3\x94", "O"}, /* Ô */ {"\xc3\x95", "O"}, /* Õ */ {"\xc3\x96", "O"}, /* Ö */ {"\xc3\xb3", "o"}, /* ó */
        {"\xc3\xb2", "o"}, /* ò */ {"\xc3\xb4", "o"}, /* ô */ {"\xc3\xb5", "o"}, /* õ */ {"\xc2\xba", "o"}, /* º */ {"\xc3\xb6", "o"}, /* ö */
        {"\xc3\x89", "E"}, /* É */ {"\xc3\x88", "E"}, /* È */ {"\xc3\x8a", "E"}, /* Ê */ {"\xc3\x8b", "E"}, /* Ë */ {"\xc3\xa9", "e"}, /* é */
        {"\xc3\xa8", "e"}, /* è */ {"\xc3\xaa", "e"}, /* ê */ {"\xc3\xab", "e"}, /* ë */ {"\xc3\xad", "i"}, /* í */ {"\xc3\xac", "i"}, /* ì */
        {"\xc3\xae", "i"}, /* î */ {"\xc3\xaf", "i"}, /* ï */ {"\xc3\x8d", "I"}, /* Í */ {"\xc3\x8c", "I"}, /* Ì */ {"\xc3\x8e", "I"}, /* Î */
        {"\xc3\x8f", "I"}, /* Ï */ {"\xc3\x81", "A"}, /* Á */ {"\xc3\x80", "A"}, /* À */ {"\xc3\x82", "A"}, /* Â */ {"\xc3\x83", "A"}, /* Ã */
        {"\xc3\x84", "A"}, /* Ä */ {"\xc3\xa1", "a"}, /* á */ {"\xc3\xa0", "a"}, /* à */ {"\xc3\xa2", "a"}, /* â */ {"\xc3\xa3", "a"}, /* ã */
        {"\xc2\xaa", "a"}, /* ª */ {"\xc3\xa4", "a"}, /* ä */ {"\xd0\x99","Y"}, /* Й */ {"\xd0\xb9","y"}, /* й */ {"\xd0\xa6","Ts"}, /* Ц */
        {"\xd1\x86","ts"}, /* ц */ {"\xd0\xa3","U"}, /* У */ {"\xd1\x83","u"}, /* у */ {"\xd0\x9a","K"}, /* К */ {"\xd0\xba","k"}, /* к */
        {"\xd0\x95","E"}, /* Е */ {"\xd0\xb5","e"}, /* е */ {"\xd0\x9d","N"}, /* Н */ {"\xd0\xbd","n"}, /* н */ {"\xd0\x93","G"}, /* Г */
        {"\xd0\xb3","g"}, /* г */ {"\xd0\xa8","Sh"}, /* Ш */ {"\xd1\x88","sh"}, /* ш */ {"\xd0\xa9","Shch"}, /* Щ */ {"\xd1\x89","shch"}, /* щ */
        {"\xd0\x97","Z"}, /* З */ {"\xd0\xb7","z"}, /* з */ {"\xd0\xa5","Kh"}, /* Х */ {"\xd1\x85","kh"}, /* х */ {"\xd0\xaa",""}, /* Ъ */
        {"\xd1\x8a",""}, /* ъ */ {"\xd0\x81","E"}, /* Ё */ {"\xd1\x91","e"}, /* ё */ {"\xd0\xa4","F"}, /* Ф */ {"\xd1\x84","f"}, /* ф */
        {"\xd0\xab","Y"}, /* Ы */ {"\xd1\x8b","y"}, /* ы */ {"\xd0\x92","V"}, /* В */ {"\xd0\xb2","v"}, /* в */ {"\xd0\x90","A"}, /* А */
        {"\xd0\xb0","a"}, /* а */ {"\xd0\x9f","P"}, /* П */ {"\xd0\xbf","p"}, /* п */ {"\xd0\xa0","R"}, /* Р */ {"\xd1\x80","r"}, /* р */
        {"\xd0\x9e","O"}, /* О */ {"\xd0\xbe","o"}, /* о */ {"\xd0\x9b","L"}, /* Л */ {"\xd0\xbb","l"}, /* л */ {"\xd0\x94","D"}, /* Д */
        {"\xd0\xb4","d"}, /* д */ {"\xd0\x96","Zh"}, /* Ж */ {"\xd0\xb6","zh"}, /* ж */ {"\xd0\xad","E"}, /* Э */ {"\xd1\x8d","e"}, /* э */
        {"\xd0\xaf","Ya"}, /* Я */ {"\xd1\x8f","ya"}, /* я */ {"\xd0\xa7","Ch"}, /* Ч */ {"\xd1\x87","ch"}, /* ч */ {"\xd0\xa1","S"}, /* С */
        {"\xd1\x81","s"}, /* с */ {"\xd0\x9c","M"}, /* М */ {"\xd0\xbc","m"}, /* м */ {"\xd0\x98","I"}, /* И */ {"\xd0\xb8","i"}, /* и */
        {"\xd0\xa2","T"}, /* Т */ {"\xd1\x82","t"}, /* т */ {"\xd0\xac",""}, /* Ь */ {"\xd1\x8c",""}, /* ь */ {"\xd0\x91","B"}, /* Б */
        {"\xd0\xb1","b"}, /* б */ {"\xd0\xae","Yu"}, /* Ю */ {"\xd1\x8e","yu"} /* ю */
    };
    static const size_t symb_size = sizeof(symbols) / sizeof(symbols[0]);
    out = in;
    for (int i = 0; i < symb_size; ++i) {
        size_t n = std::string::npos;
        while ((n = out.find(symbols[i][0])) != std::string::npos) {
            out.replace(n, strlen(symbols[i][0]), symbols[i][1]);
        }
    }

    string out_r; out_r.reserve(out.size());

    for (const char& c : out) {
        if (c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' || c == '-' || c >= '0' && c <= '9') {
            out_r += c;
        }
    }
    out_r.swap(out);
    return out;
}

std::string generateResultFileName(const std::string& name) {
    std::time_t t = std::time(nullptr);
    const std::tm* tm = std::localtime(&t);
    std::stringstream ss;
    ss << std::put_time(tm, REPLAY_FILEMASK) << name;
    std::string out_s; out_s.reserve(256);
    utf8to_translit(ss.str(), out_s);
    out_s += ".json";
    LOG(TRACE) << ss.str() << out_s;
    return out_s;
}

#pragma region Вычитка конфига

template <typename T>
T& read_config(const json& js, const char* name, T& set_to) {
    if (!js.is_object() || !js.contains(name) || js[name].is_null()) {
        LOG(WARNING) << name << "is missing in config file!";
        return set_to;
    }
    json tst = T();
    if (js[name].type_name() != tst.type_name()) {
        LOG(WARNING) << name << "have type:" << js[name].type_name() << " but expected:" << tst.type_name();
        return set_to;
    }
    set_to = js[name].get<T>();
    LOG(INFO) << "Config parameter [" << name << ":" << set_to << "]";
    return set_to;
}

#ifdef _WIN32
fs::path get_config_path_win(HMODULE hModule) {
    wchar_t szPath[MAX_PATH], szDirPath[_MAX_DIR];
    GetModuleFileNameW(hModule, szPath, MAX_PATH);
    _wsplitpath_s(szPath, 0, 0, szDirPath, _MAX_DIR, 0, 0, 0, 0);
    fs::path res{ szDirPath };
    return res;
}
#else
fs::path get_config_path() {
    Dl_info dl_info;
    if (!dladdr((void*)RVExtension, &dl_info))
        LOG(WARNING) << "OcapReplaySaver2.so: error while determiing .so path! " << dl_info.dli_fname;
    LOG(INFO) << "OcapReplaySaver2.so: " << dl_info.dli_fname;
    return fs::path(dl_info.dli_fname);
}
#endif

void readWriteConfig(fs::path cfg_path = "/etc/ocap") {
    try {
#ifndef _WIN32
        fs::path cfg_so_path = get_config_path(); cfg_so_path.remove_filename();
        cfg_path = cfg_so_path;
#endif
        fs::path cfg_name = cfg_path;
        cfg_name += "/OcapReplaySaver2.cfg.json"; cfg_name.make_preferred();
        fs::path cfg_name_sample = cfg_path; cfg_name_sample += "/OcapReplaySaver2.cfg.json.sample"; cfg_name_sample.make_preferred();

        if (!std::ifstream(cfg_name_sample)) {
            LOG(INFO) << "Creating sample config file: " << cfg_name_sample;

            json j = {
                { "httpRequestTimeout", config.httpRequestTimeout },
                { "traceLog", config.traceLog},
                { "newUrl", config.newUrl},
                { "newServerGameType", config.newServerGameType },
                { "newUrlRequestSecret", config.newUrlRequestSecret},
                { "logsDir", config.logsDir},
                { "logAndTmpPrefix", config.logAndTmpPrefix},
            };
            std::ofstream out(cfg_name_sample, ofstream::out | ofstream::binary);
            out << j.dump(4) << endl;
        }
        LOG(INFO) << "Trying to read config file:" << cfg_name;
        LOG(INFO) << cfg_name.native();
        ifstream cfg(cfg_name, ifstream::in | ifstream::binary);

        json jcfg;
        if (!cfg.is_open()) {
            LOG(WARNING) << "Cannot open cfg file! Using default params.";
            return;
        }
        try {
            cfg >> jcfg;
        }
        catch (const exception& ex)
        {
            LOG(WARNING) << "Error parsing config:" << ex.what();
        }

        read_config(jcfg, "httpRequestTimeout", config.httpRequestTimeout);
        read_config(jcfg, "traceLog", config.traceLog);
        read_config(jcfg, "newUrl", config.newUrl);
        read_config(jcfg, "newServerGameType", config.newServerGameType);
        read_config(jcfg, "newUrlRequestSecret", config.newUrlRequestSecret);
        read_config(jcfg, "logsDir", config.logsDir);
        read_config(jcfg, "logAndTmpPrefix", config.logAndTmpPrefix);
    }
    catch (const exception& ex) {
        LOG(ERROR) << "Exception:" << ex.what();
    }
    catch (...) {
        LOG(ERROR) << "Unknown exception";
    }
}
#pragma endregion


#pragma region CURL

void log_curl_exe_string(const string& b_url, const string& worldname, const string& missionName,
    const string& missionDuration, const string& filename, const std::pair<std::string, std::optional<std::string> >& pair_files,
    int timeout, const string& gametype, const string& secret) {
    stringstream ss;
    ss << "curl ";
    ss << "-F file=\"@" << (pair_files.second ? *pair_files.second : pair_files.first) << "\" ";
    ss << "-F filename=\"" << filename << "\" ";
    ss << "-F worldName=\"" << worldname << "\" ";
    ss << "-F missionName=\"" << missionName << "\" ";
    ss << "-F missionDuration=\"" << missionDuration << "\" ";
    ss << "-F type=\"" << gametype << "\" ";
    ss << "-F secret=\"" << "your_password" << "\" ";
    ss << b_url;
    LOG(INFO) << "String for reupload: " << ss.str();
}

void curlUploadNew(const string& b_url, const string& worldname, const string& missionName, const string& missionDuration, const string& filename, const pair<std::string, std::optional<std::string> >& pair_files, int timeout, const string& gametype, const string& secret) {
    LOG(INFO) << b_url << worldname << missionName << missionDuration << filename << (pair_files.second ? *pair_files.second : pair_files.first) << timeout << gametype;
    log_curl_exe_string(b_url, worldname, missionName, missionDuration, filename, pair_files, timeout, gametype, secret);
    CURL* curl{ nullptr };
    CURLcode res{ CURLE_OK };
    string file = pair_files.second ? *pair_files.second : pair_files.first;
    static const char buf[] = "Expect:";
    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, b_url.c_str());
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)timeout);
        curl_easy_setopt(curl, CURLOPT_DEFAULT_PROTOCOL, "https");
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, buf);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_mime* mime;
        curl_mimepart* part;
        mime = curl_mime_init(curl);
        part = curl_mime_addpart(mime);
        curl_mime_name(part, "file");
        curl_mime_filedata(part, file.c_str());
        part = curl_mime_addpart(mime);
        curl_mime_name(part, "filename");
        curl_mime_data(part, filename.c_str(), CURL_ZERO_TERMINATED);
        part = curl_mime_addpart(mime);
        curl_mime_name(part, "worldName");
        curl_mime_data(part, worldname.c_str(), CURL_ZERO_TERMINATED);
        part = curl_mime_addpart(mime);
        curl_mime_name(part, "missionName");
        curl_mime_data(part, missionName.c_str(), CURL_ZERO_TERMINATED);
        part = curl_mime_addpart(mime);
        curl_mime_name(part, "missionDuration");
        curl_mime_data(part, missionDuration.c_str(), CURL_ZERO_TERMINATED);
        part = curl_mime_addpart(mime);
        curl_mime_name(part, "type");
        curl_mime_data(part, gametype.c_str(), CURL_ZERO_TERMINATED);
        part = curl_mime_addpart(mime);
        curl_mime_name(part, "secret");
        curl_mime_data(part, secret.c_str(), CURL_ZERO_TERMINATED);
        curl_easy_setopt(curl, CURLOPT_MIMEPOST, mime);
        res = curl_easy_perform(curl);
        stringstream total;
        if (!res) {
            curl_off_t ul;
            double ttotal;
            res = curl_easy_getinfo(curl, CURLINFO_SIZE_UPLOAD_T, &ul);
            if (!res)
                total << "Uploaded " << ul << " bytes";
            res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &ttotal);
            if (!res)
                total << " in " << ttotal << " sec.";
        }
        total << " URL:" << b_url;

        if (res != CURLE_OK)
            LOG(ERROR) << "Curl error:" << curl_easy_strerror(res) << total.str();
        else
            LOG(INFO) << "Curl OK:" << total.str();
        curl_easy_cleanup(curl);
        curl_mime_free(mime);
        curl_slist_free_all(headers);
    }
}


void curlActions(string worldName, string missionName, string duration, string filename, pair<std::string, std::optional<std::string> > tfile) {
    if (!curl_init) {
        curl_global_init(CURL_GLOBAL_ALL);
        curl_init = true;
    }
    curlUploadNew(config.newUrl, worldName, missionName, duration, filename, tfile, config.httpRequestTimeout, mission_type, config.newUrlRequestSecret);
    LOG(INFO) << "Finished!";
}

#pragma endregion

#pragma region Commands Handlers


/*
Input parameters:
 0  marker name
 1  marker direction
 2  swt_cfgMarkers_names select _type, markerType, "mil_dot"
 3  marker text
 4  ocap_captureFrameNo, frame when marker was created
 5  -1, frame when marker was deleted
 6  _pl getVariable ["ocap_id", 0], who has placed the marker
 7  call bis_fnc_colorRGBtoHTML, "0000FF"
 8  marker size [1,1]
 9  side _pl call BIS_fnc_sideID, -1 global, 0 - east, 1 - west , 2 - resistance
 10 markerPos, [100,100]
 11 Shape (opt.), "ICON"
 12 markerAlpha (opt.), 100
 13 markerBrush (opt.), "Solid"

*/

// :MARKER:CREATE: 11:["SWT_M#156"::0::"o_inf"::"CBR"::0::-1::0::"#0000FF"::[1,1]::0::[3915.44,1971.98]]
// :MARKER:CREATE: 12:["SWT_M#156"::0::"o_inf"::"CBR"::0::-1::0::"#0000FF"::[1,1]::0::[3915.44,1971.98]::"ICON"]
// :MARKER:CREATE: 13:["SWT_M#156"::0::"o_inf"::"CBR"::0::-1::0::"#0000FF"::[1,1]::0::[3915.44,1971.98]::"ICON"::100]
// :MARKER:CREATE: 14:["SWT_M#156"::0::"o_inf"::"CBR"::0::-1::0::"#0000FF"::[1,1]::0::[3915.44,1971.98]::"ICON"::100::"Solid"]

void commandMarkerCreate(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS_N(11, 14);
    COMMAND_CHECK_WRITING_STATE;

    //создать новый маркер
    if (j["Markers"].is_null()) {
        j["Markers"] = json::array();
    }
    /* входные параметры
    [0:_mname , 1: 0, 2 : swt_cfgMarkers_names select _type, 3: _mtext, 4: ocap_captureFrameNo, 5:-1, 6: _pl getVariable ["ocap_id", 0],
    7: call bis_fnc_colorRGBtoHTML, 8: marker size [1,1], 9:side _pl call BIS_fnc_sideID, 10:_mpos, 11: Shape (opt.), 12: markerAlpha (opt.)]]
    */

    json::string_t clr = json::string_t(removeHash(filterSqfString(args[7])));
    if (clr == "any") {
        clr = "000000";
    }
    json frameNo = JSON_INT_FROM_ARG(4);

    json a = json::array({
        /*0*/	JSON_STR_FROM_ARG(2), // markerType
        /*1*/	JSON_STR_FROM_ARG(3), // markerText
        /*2*/	frameNo, // Frame number when marker created
        /*3*/	JSON_INT_FROM_ARG(5), // -1 by default, frame when marker deleted
        /*4*/	JSON_INT_FROM_ARG(6), //  ocap_id
        /*5*/	clr, // Color
        /*6*/	JSON_INT_FROM_ARG(9), // side
        /*7*/	json::array(), // marker position on frames	[[frame, [x,y], dir],[frame, [x,y], dir], ...]
        /*8*/	JSON_PARSE_FROM_ARG(8), // Marker size	[1,1]
        /*9*/	JSON_STR_FROM_ARG(0) // MarkerId
        }); // Marker pos
    if (args.size() > 11) {
        json shape = JSON_STR_FROM_ARG(11);
        a.push_back(shape);
    }
    if (args.size() > 13) {
        json brush = JSON_STR_FROM_ARG(13);
        a.push_back(brush);
    }

    //json alpha = args.size() < 13 ? ((shape == "RECTANGLE" || shape == "ELLIPSE") ? json::number_integer_t(20) : json::number_integer_t(100)) : JSON_INT_FROM_ARG(12);

    //                                           markerPos                dir                    alpha
    json coordRecord = json::array({ frameNo, JSON_PARSE_FROM_ARG(10), JSON_FLOAT_FROM_ARG(1) });
    if (args.size() > 12) {
        json alpha = JSON_INT_FROM_ARG(12);
        coordRecord.push_back(alpha);
    }

    a[7].push_back(coordRecord);
    j["Markers"].push_back(a);
}

// :MARKER:DELETE: 2:["SWT_M#126"::0]
void commandMarkerDelete(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS(2);
    COMMAND_CHECK_WRITING_STATE;
    json mname = JSON_STR_FROM_ARG(0);
    auto it = find_if(j["Markers"].rbegin(), j["Markers"].rend(), [&](const auto& i) { return i[9] == mname; });
    if (it == j["Markers"].rend()) {
        LOG(ERROR) << "No such marker" << mname;
        throw ocapException("No such marker!");
    }
    // find old marker and set to it current frame number
    (*it)[3] = JSON_INT_FROM_ARG(1);
}

// :SET:VERSION: 1:["Ocap addon version 1.1.1.1"]
void commandSetVersion(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS(1);
    addon_version = JSON_STR_FROM_ARG(0);
    LOG(INFO) << "Set addon version:" << addon_version;
    if (is_writing)
        j["addonVersion"] = addon_version;
}

// markerName, frame, position, (opt.) dir , (opt.) alpha
// :MARKER:MOVE: 3:["SWT_M#156"::0::[3882.53,2041.32]]
// :MARKER:MOVE: 4:["SWT_M#156"::0::[3882.53,2041.32]::180.0]
// :MARKER:MOVE: 5:["SWT_M#156"::0::[3882.53,2041.32]::180.0::100]
void commandMarkerMove(const vector<string>& args) {
    COMMAND_CHECK_WRITING_STATE;
    COMMAND_CHECK_INPUT_PARAMETERS_N(3, 5);

    json dir = args.size() < 4 ? json::number_float_t(0) : JSON_FLOAT_FROM_ARG(3);

    json mname = JSON_STR_FROM_ARG(0); // имя маркера
    auto it = find_if(j["Markers"].rbegin(), j["Markers"].rend(), [&](const auto& i) { return i[9] == mname; });
    if (it == j["Markers"].rend()) {
        LOG(ERROR) << "No such marker" << mname;
        throw ocapException("No such marker!");
    }
    json coordRecord = json::array({ JSON_INT_FROM_ARG(1), JSON_PARSE_FROM_ARG(2), dir });
    if (args.size() > 4) {
        json alpha = JSON_INT_FROM_ARG(4);
        coordRecord.push_back(alpha);
    }
    // ищем последнюю запись с таким же фреймом
    int frame = coordRecord[0];
    auto coord = find_if((*it)[7].rbegin(), (*it)[7].rend(), [&](const auto& i) { return i[0] == frame; });
    if (coord == (*it)[7].rend()) {
        // такой записи нет
        LOG(TRACE) << "No marker coord on this frame. Adding new." << coordRecord;
        (*it)[7].push_back(coordRecord);
    }
    else
    {
        LOG(TRACE) << "Record on this frame already exists." << *coord << "replacing it with new params" << coordRecord;
        *coord = coordRecord;
    }
}

void commandLog(const vector<string>& args) {
    stringstream ss;
    for (int i = 0; i < args.size(); i++)
        ss << args[i] << " ";
    CLOG(WARNING, "ext") << ss.str();
}

// TRACE :FIRED: 3:[116::147::[3728.17,2999.07]]
void commandFired(const vector<string>& args)
{
    COMMAND_CHECK_INPUT_PARAMETERS(3);
    COMMAND_CHECK_WRITING_STATE;

    int id = stoi(args[0]);
    if (!j["entities"][id].is_null()) {
        j["entities"][id]["framesFired"].push_back(json::array({
            JSON_INT_FROM_ARG(1),
            JSON_PARSE_FROM_ARG(2)
            }));
    }
    else {
        LOG(ERROR) << "Incorrect params, no" << id << "entity!";
    }
}

// START: 4:["Woodland_ACR"::"RBC_194_Psy_woiny_13a"::"[TF]Shatun63"::1.23]
void commandStart(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS(4);

    if (is_writing) {
        LOG(WARNING) << ":START: while writing mission. Clearing old data.";
        commandClear(args);
    }
    LOG(INFO) << "Closing old log. Starting record." << args[0] << args[1] << args[2] << args[3];

    auto loggerDefault = el::Loggers::getLogger("default");
    auto loggerExt = el::Loggers::getLogger("ext");

    if (loggerDefault && loggerExt) {

        loggerDefault->reconfigure();
        loggerExt->reconfigure();
    }
    else {
        LOG(ERROR) << "Problem with reconfiguring loggers!";
    }

    is_writing = true;
    j["worldName"] = JSON_STR_FROM_ARG(0);
    j["missionName"] = JSON_STR_FROM_ARG(1);
    j["missionAuthor"] = JSON_STR_FROM_ARG(2);
    j["captureDelay"] = JSON_FLOAT_FROM_ARG(3);

    string version{ CURRENT_VERSION };
    j["extensionVersion"] = version;
    j["extensionBuild"] = __TIMESTAMP__;
    if (!addon_version.empty())
        j["addonVersion"] = addon_version;

    mission_type = config.newServerGameType;

    LOG(INFO) << "Starting record." << args[0] << args[1] << args[2] << args[3];
    CLOG(INFO, "ext") << "Starting record." << args[0] << args[1] << args[2] << args[3];
}

// TIME: 5:[0,"2000-12-31T23:59:59.000"::"2000-12-31T23:59:59.000"::1.0::120.203]
void commandTime(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS(5);
    COMMAND_CHECK_WRITING_STATE;

    json time{
        { "frameNum", JSON_INT_FROM_ARG(0) },
        { "systemTimeUTC", JSON_STR_FROM_ARG(1) },
        { "date", JSON_STR_FROM_ARG(2) },
        { "timeMultiplier", JSON_FLOAT_FROM_ARG(3) },
        { "time", JSON_FLOAT_FROM_ARG(4) }
    };
    j["times"].push_back(time);
}

// :NEW:UNIT: 6:[0::0::"|UN|Capt.Farid"::"Alpha 1-1"::"EAST"::1]
// :NEW:UNIT: 7:[0::0::"|UN|Capt.Farid"::"Alpha 1-1"::"EAST"::1::"Soldiers@Soldier"]
void commandNewUnit(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS_N(6, 7);
    COMMAND_CHECK_WRITING_STATE;

    json unit{
        { "startFrameNum", JSON_INT_FROM_ARG(0) },
        { "type" , "unit" },
        { "id", JSON_INT_FROM_ARG(1) },
        { "name", JSON_STR_FROM_ARG(2) },
        { "group", JSON_STR_FROM_ARG(3) },
        { "side", JSON_STR_FROM_ARG(4) },
        { "isPlayer", JSON_INT_FROM_ARG(5) }
    };
    unit["positions"] = json::array();
    unit["framesFired"] = json::array();
    if (args.size() > 6) {
        unit["role"] = JSON_STR_FROM_ARG(6);
    }
    j["entities"].push_back(unit);
}

// :NEW:VEH: 4:[0::204::"plane"::"MQ-4A Greyhawk"]
void commandNewVeh(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS(4);
    COMMAND_CHECK_WRITING_STATE;

    json unit{
        { "startFrameNum", JSON_INT_FROM_ARG(0) },
        { "type" , "vehicle" },
        { "id", JSON_INT_FROM_ARG(1) },
        { "name", JSON_STR_FROM_ARG(3) },
        { "class", JSON_STR_FROM_ARG(2) }
    };
    unit["positions"] = json::array();
    unit["framesFired"] = json::array();
    j["entities"].push_back(unit);
}

// :SAVE: 5:["Beketov"::"RBC 202 Неожиданный поворот 05"::"[RE]Aventador"::1.23::4233]
// :SAVE: 6:["Beketov"::"RBC 202 Неожиданный поворот 05"::"[RE]Aventador"::1.23::4233::"TvT"]
void commandSave(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS_N(5, 6);
    COMMAND_CHECK_WRITING_STATE;

    LOG(INFO) << args[0] << args[1] << args[2] << args[3] << args[4];

    j["worldName"] = JSON_STR_FROM_ARG(0);
    j["missionName"] = JSON_STR_FROM_ARG(1);
    j["missionAuthor"] = JSON_STR_FROM_ARG(2);
    j["captureDelay"] = JSON_FLOAT_FROM_ARG(3);
    j["endFrame"] = JSON_INT_FROM_ARG(4);
    if (args.size() > 5) {
        j["tags"] = JSON_STR_FROM_ARG(5);
        mission_type = JSON_STR_FROM_ARG(5);
        LOG(INFO) << mission_type;
    }
    prepareMarkerFrames(j["endFrame"]);
    auto fnames = saveCurrentReplayToTempFile();
    LOG(INFO) << "TMP:" << fnames.first;

    string fname = generateResultFileName(j["missionName"]);
    curlActions(j["worldName"], j["missionName"], to_string(stod(args[3]) * stod(args[4])), fname, fnames);

    return commandClear(args);
}


void commandClear(const vector<string>& args)
{
    COMMAND_CHECK_WRITING_STATE;
    LOG(INFO) << "CLEAR";
    j.clear();
    is_writing = false;
}

// :UPDATE:UNIT: 7:[0::[14548.4,19793.9]::84::1::0::"|UN|Capt.Farid"::1]
// :UPDATE:UNIT: 8:[0::[14548.4,19793.9]::84::1::0::"|UN|Capt.Farid"::1::"Medic"]
void commandUpdateUnit(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS_N(7, 8);
    COMMAND_CHECK_WRITING_STATE;

    int id = stoi(args[0]);
    if (!j["entities"][id].is_null()) {
        json pos_update = json::array({
                   JSON_PARSE_FROM_ARG(1),
                   JSON_INT_FROM_ARG(2),
                   JSON_INT_FROM_ARG(3),
                   JSON_INT_FROM_ARG(4),
                   JSON_STR_FROM_ARG(5),
                   JSON_INT_FROM_ARG(6) });
        if (args.size() > 7)
            pos_update.push_back(JSON_STR_FROM_ARG(7));

        j["entities"][id]["positions"].push_back(pos_update);
    }
    else {
        LOG(ERROR) << "Incorrect params, no" << id << "entity!";
    }
}

// :UPDATE:VEH: 5:[204::[2099.44,6388.62,0]::0::1::[202,203]]
// :UPDATE:VEH: 6:[204::[2099.44,6388.62,0]::0::1::[202,203]::1]
void commandUpdateVeh(const vector<string>& args) {
    COMMAND_CHECK_INPUT_PARAMETERS_N(5, 6);
    COMMAND_CHECK_WRITING_STATE;

    int id = stoi(args[0]);
    if (!j["entities"][id].is_null()) {
        json arr = json::array({
            JSON_PARSE_FROM_ARG(1),
            JSON_INT_FROM_ARG(2),
            JSON_INT_FROM_ARG(3),
            JSON_PARSE_FROM_ARG(4)
        });

        bool createNew = true;
        if (args.size() > 5) {
            int frameNo = stoi(args[5]);
            arr.push_back(json::array({
                JSON_INT_FROM_ARG(5), // start frame
                JSON_INT_FROM_ARG(5) // end frame
                }));

            if (auto lastPosData = j["entities"][id]["positions"].rbegin(); lastPosData != j["entities"][id]["positions"].rend()) {
                auto& lastPosArr = *lastPosData;
                LOG(TRACE) << lastPosArr.dump(-1, ' ', false, json::error_handler_t::ignore) << arr.dump(-1, ' ', false, json::error_handler_t::ignore);

                if (lastPosArr[0] == arr[0] && lastPosArr[1] == arr[1] && lastPosArr[2] == arr[2] && lastPosArr[3] == arr[3]) {
                    LOG(TRACE) << "no vehicle change detected, extending previous frame";

                    createNew = false;
                    lastPosArr[4][1] = arr[4][1];
                }
            }
        }
        if (createNew) {
            j["entities"][id]["positions"].push_back(arr);
        }
    }
    else {
        LOG(ERROR) << "Incorrect params, no" << id << "entity!";
    }
}

// :EVENT: 3:[0::"connected"::"[RMC] DoS"]
// :EVENT: 5:[404::"killed"::84::[83, "AKS-74N"]::10]
// :EVENT: 3:[3312::"disconnected"::"[VRG] mEss1a"]
// :EVENT: 5:[3652::"killed"::160::[83, "ПКП ""Печенег"""]::80]
// :EVENT: 3:[4939::"endMission"::["EAST","OPFOR Wins. Their enemies suffered heavy losses!"]]
void commandEvent(const vector<string>& args)
{
    COMMAND_CHECK_WRITING_STATE;
    if (args.size() < 3) {
        ERROR_THROW("Number of arguments lesser then 3!")
    }
    json arr = json::array();
    for (int i = 0; i < args.size(); i++) {
        arr.push_back(JSON_PARSE_FROM_ARG(i));
    }
    j["events"].push_back(arr);
}

#pragma endregion

void initialize_logger(int verb_level = 0) {
    try {
        fs::path logName = getAndCreateLogDirectory();
        fs::path logNameExt = logName;
        logName += "/"; logName += config.logAndTmpPrefix; logName += "main.%datetime{%Y%M%d_%H%m%s}.log"; logName.make_preferred();
        logNameExt += "/"; logNameExt += config.logAndTmpPrefix; logNameExt += "ext.%datetime{%Y%M%d_%H%m%s}.log"; logNameExt.make_preferred();
        LOG(INFO) << "logifles: " << logName << ":" << logNameExt;

        el::Configurations defaultConf;
        defaultConf.setToDefault();
        defaultConf.setGlobally(el::ConfigurationType::ToFile, "true");
        defaultConf.setGlobally(el::ConfigurationType::Enabled, "true");
        defaultConf.setGlobally(el::ConfigurationType::Filename, logName.string());

        defaultConf.setGlobally(el::ConfigurationType::ToStandardOutput, "false");
        defaultConf.setGlobally(el::ConfigurationType::Format, "%datetime %thread [%fbase:%line:%func] %level %msg");
        defaultConf.setGlobally(el::ConfigurationType::MillisecondsWidth, "4");
        defaultConf.setGlobally(el::ConfigurationType::MaxLogFileSize, "104857600"); // 100 Mb
        defaultConf.setGlobally(el::ConfigurationType::LogFlushThreshold, "100");

        defaultConf.set(el::Level::Trace, el::ConfigurationType::Enabled, "false");

        el::Configurations externalConf;
        externalConf.setToDefault();
        externalConf.setFromBase(&defaultConf);
        externalConf.setGlobally(el::ConfigurationType::Filename, logNameExt.string());
        externalConf.setGlobally(el::ConfigurationType::Format, "%datetime %msg");

        //el::Loggers::reconfigureAllLoggers(defaultConf);
        //el::Loggers::setDefaultConfigurations(defaultConf);
        el::Loggers::reconfigureLogger(el::Loggers::getLogger("default", true), defaultConf);
        el::Loggers::addFlag(el::LoggingFlag::DisableApplicationAbortOnFatalLog);
        el::Loggers::addFlag(el::LoggingFlag::AutoSpacing);
        el::Loggers::addFlag(el::LoggingFlag::ImmediateFlush);
        //el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
        el::Loggers::setVerboseLevel(verb_level);
        //	el::Loggers::addFlag(el::LoggingFlag::HierarchicalLogging);
        el::Helpers::validateFileRolling(el::Loggers::getLogger("default"), el::Level::Info);

        el::Loggers::reconfigureLogger(el::Loggers::getLogger("ext", true), externalConf);
        el::Helpers::validateFileRolling(el::Loggers::getLogger("ext"), el::Level::Info);

        if (config.traceLog) {
            el::Configurations defaultConf(*el::Loggers::getLogger("default")->configurations());
            defaultConf.set(el::Level::Trace, el::ConfigurationType::Enabled, "true");
            el::Loggers::reconfigureLogger(el::Loggers::getLogger("default", true), defaultConf);
        }

        LOG(INFO) << "Logging initialized " << CURRENT_VERSION << " build: " << __TIMESTAMP__;
        CLOG(INFO, "ext") << "External logging initialized " << CURRENT_VERSION << " build: " << __TIMESTAMP__;
    }
    catch (const exception& ex) {
        LOG(ERROR) << "Exception:" << ex.what();
    }
    catch (...) {
        LOG(ERROR) << "Unknown exception";
    }
}


void __stdcall RVExtensionVersion(char* output, int outputSize)
{
    strncpy(output, CURRENT_VERSION, outputSize);
}

void __stdcall RVExtension(char* output, int outputSize, const char* function)
{
    string str_function(function);
    if (str_function == CMD_VERSION) {
        stringstream sstmp;
        sstmp << "[\"" << CURRENT_VERSION << "\",\"" << __TIMESTAMP__ << "\"]";
        string version{ sstmp.str() };
        LOG(TRACE) << "Requested version:" << version;
        strncpy(output, version.c_str(), outputSize);
        return;
    }
    LOG(ERROR) << "IN:" << function << " OUT:" << "Error: Not supported call";
    strncpy(output, "Error: Not supported call", outputSize);
}

int __stdcall RVExtensionArgs(char* output, int outputSize, const char* function, const char** args, int argsCnt)
{
    int res = 0;
    if (config.traceLog) {
        stringstream ss;
        ss << function << " " << argsCnt << ":[";
        for (int i = 0; i < argsCnt; i++) {
            if (i > 0)
                ss << "::";
            ss << args[i];
        }
        ss << "]";
        LOG(TRACE) << ss.str();
    }

    try {
        string str_function(function);
        vector<string> str_args;

        if (str_function == CMD_VERSION) {
            stringstream sstmp;
            sstmp << "[\"" << CURRENT_VERSION << "\",\"" << __TIMESTAMP__ << "\"]";
            string version{ sstmp.str() };
            LOG(TRACE) << "Requested version:" << version;
            strncpy(output, version.c_str(), outputSize);
            return res;
        }

        for (int i = 0; i < argsCnt; ++i)
        {
            str_args.push_back(string(args[i]));
        }
        {
            unique_lock<mutex> lock(command_mutex);
            commands.push(std::make_tuple(std::move(str_function), std::move(str_args)));
        }
        if (!command_thread.joinable())
        {
            LOG(TRACE) << "No worker thread. Creating one!";
            command_thread = thread(command_loop);
        }
        command_cond.notify_one();
    }
    catch (const exception& e)
    {
        LOG(ERROR) << "Exception: " << e.what();
    }
    catch (...)
    {
        LOG(ERROR) << "Exception: Unknown";
    }

    return res;
}


#ifdef _WIN32
// Normal Windows DLL junk...
BOOL APIENTRY DllMain(HMODULE hModule,
    DWORD  ul_reason_for_call,
    LPVOID lpReserved
)
{
    switch (ul_reason_for_call)
    {
    case DLL_PROCESS_ATTACH: {
        readWriteConfig(get_config_path_win(hModule));
        initialize_logger();
        break;
    }

    case DLL_THREAD_ATTACH: break;
    case DLL_THREAD_DETACH: break;
    case DLL_PROCESS_DETACH: {
        if (curl_init)
            curl_global_cleanup();
        command_thread_shutdown = true;
        command_cond.notify_one();
        if (command_thread.joinable())
            command_thread.join();
        break;

    }
    }
    return TRUE;
}
#else

struct MainStatic final {
    MainStatic() {
        readWriteConfig();
        initialize_logger();
    }
    ~MainStatic() {
        if (curl_init)
            curl_global_cleanup();
        command_thread_shutdown = true;
        command_cond.notify_one();
        if (command_thread.joinable())
            command_thread.join();
    }
} main_static;

#endif
