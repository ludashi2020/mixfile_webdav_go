package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// 配置常量
const (
	API_BASE    = "http://127.0.0.1:4719/api/"
	SERVER_PORT = 1900
	DATA_FILE   = "webdav.dat"
)

// 文件系统结构
type FileEntry struct {
	Path      string `json:"path"`
	Content   string `json:"content"` // 仅文件有内容（shareCode）
	IsDir     bool   `json:"isDir"`
	Created   int64  `json:"created"`
	Modified  int64  `json:"modified"`
}

type FileSystem struct {
	Files []FileEntry
}

// 文件系统操作
func (fs *FileSystem) GetFile(path string) (*FileEntry, error) {
	path = normalizePath(path)
	for _, entry := range fs.Files {
		if normalizePath(entry.Path) == path && !entry.IsDir {
			return &entry, nil
		}
	}
	return nil, fmt.Errorf("file not found: %s", path)
}

func (fs *FileSystem) GetDir(path string) (*FileEntry, error) {
	path = normalizePath(path)
	for _, entry := range fs.Files {
		if normalizePath(entry.Path) == path && entry.IsDir {
			return &entry, nil
		}
	}
	return nil, fmt.Errorf("directory not found: %s", path)
}

func (fs *FileSystem) PutFile(path, content string) {
	path = normalizePath(path)
	now := time.Now().Unix()
	for i, entry := range fs.Files {
		if normalizePath(entry.Path) == path && !entry.IsDir {
			fs.Files[i].Content = content
			fs.Files[i].Modified = now
			saveFileSystem(fs)
			return
		}
	}
	fs.Files = append(fs.Files, FileEntry{
		Path:     path,
		Content:  content,
		IsDir:    false,
		Created:  now,
		Modified: now,
	})
	saveFileSystem(fs)
}

func (fs *FileSystem) CreateDir(path string) {
	path = normalizePath(path)
	now := time.Now().Unix()
	for _, entry := range fs.Files {
		if normalizePath(entry.Path) == path {
			return // 已存在，无需重复创建
		}
	}
	fs.Files = append(fs.Files, FileEntry{
		Path:     path,
		Content:  "",
		IsDir:    true,
		Created:  now,
		Modified: now,
	})
	saveFileSystem(fs)
}

func (fs *FileSystem) Delete(path string) error {
	path = normalizePath(path)
	for i, entry := range fs.Files {
		if normalizePath(entry.Path) == path {
			fs.Files = append(fs.Files[:i], fs.Files[i+1:]...)
			saveFileSystem(fs)
			return nil
		}
	}
	return fmt.Errorf("path not found: %s", path)
}

// 生成唯一路径
func generateUniquePath(fs *FileSystem, dstPath string) string {
	basePath := dstPath
	ext := ""
	if !strings.HasSuffix(dstPath, "/") && strings.Contains(dstPath, ".") {
		lastDot := strings.LastIndex(dstPath, ".")
		basePath = dstPath[:lastDot]
		ext = dstPath[lastDot:]
	}
	for i := 1; ; i++ {
		newPath := fmt.Sprintf("%s (%d)%s", basePath, i, ext)
		exists := false
		for _, e := range fs.Files {
			if normalizePath(e.Path) == newPath {
				exists = true
				break
			}
		}
		if !exists {
			return newPath
		}
	}
}

func (fs *FileSystem) Move(srcPath, dstPath string) error {
	srcPath = normalizePath(srcPath)
	dstPath = normalizePath(dstPath)
	for i, entry := range fs.Files {
		if normalizePath(entry.Path) == srcPath {
			// 如果目标路径存在，生成新路径
			finalDstPath := dstPath
			for _, e := range fs.Files {
				if normalizePath(e.Path) == finalDstPath {
					finalDstPath = generateUniquePath(fs, dstPath)
					break
				}
			}
			// 如果是文件夹，收集所有子项并更新路径
			if entry.IsDir {
				oldPrefix := srcPath + "/"
				newPrefix := finalDstPath + "/"
				var subEntries []FileEntry
				for _, subEntry := range fs.Files {
					if strings.HasPrefix(normalizePath(subEntry.Path), oldPrefix) {
						newPath := newPrefix + strings.TrimPrefix(normalizePath(subEntry.Path), oldPrefix)
						log.Printf("Updating subitem: %s -> %s", subEntry.Path, newPath)
						subEntries = append(subEntries, FileEntry{
							Path:     newPath,
							Content:  subEntry.Content,
							IsDir:    subEntry.IsDir,
							Created:  subEntry.Created,
							Modified: time.Now().Unix(),
						})
					}
				}
				// 删除旧子项
				for j := len(fs.Files) - 1; j >= 0; j-- {
					if strings.HasPrefix(normalizePath(fs.Files[j].Path), oldPrefix) {
						fs.Files = append(fs.Files[:j], fs.Files[j+1:]...)
					}
				}
				// 添加新子项
				fs.Files = append(fs.Files, subEntries...)
			}
			// 更新当前条目
			fs.Files[i].Path = finalDstPath
			fs.Files[i].Modified = time.Now().Unix()
			saveFileSystem(fs)
			log.Printf("Moved %s to %s (renamed due to conflict)", srcPath, finalDstPath)
			return nil
		}
	}
	return fmt.Errorf("source path not found: %s", srcPath)
}

func (fs *FileSystem) Copy(srcPath, dstPath string) error {
	srcPath = normalizePath(srcPath)
	dstPath = normalizePath(dstPath)
	for _, entry := range fs.Files {
		if normalizePath(entry.Path) == srcPath {
			// 如果目标路径存在，生成新路径
			finalDstPath := dstPath
			for _, e := range fs.Files {
				if normalizePath(e.Path) == finalDstPath {
					finalDstPath = generateUniquePath(fs, dstPath)
					break
				}
			}
			// 创建新条目
			newEntry := FileEntry{
				Path:     finalDstPath,
				Content:  entry.Content,
				IsDir:    entry.IsDir,
				Created:  time.Now().Unix(),
				Modified: time.Now().Unix(),
			}
			fs.Files = append(fs.Files, newEntry)
			// 如果是文件夹，复制所有子项
			if entry.IsDir {
				oldPrefix := srcPath + "/"
				newPrefix := finalDstPath + "/"
				for _, subEntry := range fs.Files {
					if strings.HasPrefix(normalizePath(subEntry.Path), oldPrefix) {
						newSubEntry := FileEntry{
							Path:     newPrefix + strings.TrimPrefix(normalizePath(subEntry.Path), oldPrefix),
							Content:  subEntry.Content,
							IsDir:    subEntry.IsDir,
							Created:  time.Now().Unix(),
							Modified: time.Now().Unix(),
						}
						log.Printf("Copying subitem: %s -> %s", subEntry.Path, newSubEntry.Path)
						fs.Files = append(fs.Files, newSubEntry)
					}
				}
			}
			saveFileSystem(fs)
			log.Printf("Copied %s to %s (renamed due to conflict)", srcPath, finalDstPath)
			return nil
		}
	}
	return fmt.Errorf("source path not found: %s", srcPath)
}

func (fs *FileSystem) ListDir(path string) []FileEntry {
	var children []FileEntry
	prefix := normalizePath(path)
	if prefix == "" {
		prefix = "/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	log.Printf("Listing children for %s with prefix: %s", path, prefix)
	for _, entry := range fs.Files {
		normalizedEntryPath := normalizePath(entry.Path)
		if strings.HasPrefix(normalizedEntryPath, prefix) && normalizedEntryPath != normalizePath(path) {
			remaining := strings.TrimPrefix(normalizedEntryPath, prefix)
			if !strings.Contains(remaining, "/") {
				children = append(children, entry)
				log.Printf("Found child: %s", entry.Path)
			}
		}
	}
	return children
}

// 规范化路径：去除末尾斜杠
func normalizePath(path string) string {
	return strings.TrimSuffix(path, "/")
}

// 获取文件大小
type FileInfo struct {
	Size int64  `json:"size"`
	Name string `json:"name"`
}

func getFileSize(shareCode string) int64 {
	resp, err := http.Get(API_BASE + "file_info?s=" + url.QueryEscape(shareCode))
	if err != nil {
		log.Printf("Failed to fetch file info for shareCode %s: %v", shareCode, err)
		return 0
	}
	defer resp.Body.Close()

	var info FileInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		log.Printf("Failed to decode file info for shareCode %s: %v", shareCode, err)
		return 0
	}
	return info.Size
}

// WebDAV 处理函数
func handleWebDAV(w http.ResponseWriter, r *http.Request) {
	fs := loadFileSystem()
	path := r.URL.Path
	log.Printf("Received %s request for path: %s", r.Method, path)

	switch r.Method {
	case "GET":
		entry, err := fs.GetFile(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		shareCode := entry.Content

		downloadURL := API_BASE + "download?s=" + url.QueryEscape(shareCode)
		client := &http.Client{}
		req, err := http.NewRequest("GET", downloadURL, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}

		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		w.WriteHeader(resp.StatusCode)
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.Header().Set("x-mix-code", shareCode)
		io.Copy(w, resp.Body)

	case "PUT":
		fileName := path[strings.LastIndex(path, "/")+1:]
		contentLength := r.ContentLength
		if contentLength <= 0 {
			log.Printf("PUT request for %s failed: Content-Length is %d", path, contentLength)
			http.Error(w, "Empty file not allowed", http.StatusBadRequest)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read PUT request body for %s: %v", path, err)
			http.Error(w, "Failed to read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if len(body) == 0 {
			log.Printf("PUT request for %s failed: Body is empty despite Content-Length %d", path, contentLength)
			http.Error(w, "Empty file not allowed", http.StatusBadRequest)
			return
		}
		log.Printf("Received PUT request for %s, Content-Length: %d, Body size: %d", path, contentLength, len(body))

		uploadURL := API_BASE + "upload?name=" + url.QueryEscape(fileName)
		client := &http.Client{}
		req, err := http.NewRequest("PUT", uploadURL, bytes.NewReader(body))
		if err != nil {
			log.Printf("Failed to create upload request for %s: %v", path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		req.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Upload request to %s failed: %v", uploadURL, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		shareCode, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read upload response for %s: %v", path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		shareCodeStr := string(shareCode)
		log.Printf("Received shareCode for %s: %s", path, shareCodeStr)

		if resp.StatusCode != http.StatusOK || len(shareCodeStr) < 10 || strings.Contains(shareCodeStr, "不合法") {
			log.Printf("Upload failed for %s: %s (Status: %d)", path, shareCodeStr, resp.StatusCode)
			http.Error(w, "Upload failed: "+shareCodeStr, http.StatusBadRequest)
			return
		}

		fs.PutFile(path, shareCodeStr)
		log.Printf("文件上传成功: %s", shareCodeStr)
		w.WriteHeader(http.StatusOK)

	case "MKCOL":
		fs.CreateDir(path)
		log.Printf("Directory created: %s", path)
		w.WriteHeader(http.StatusCreated)

	case "DELETE":
		err := fs.Delete(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		log.Printf("Deleted path: %s", path)
		w.WriteHeader(http.StatusNoContent)

	case "MOVE":
		destHeader := r.Header.Get("Destination")
		if destHeader == "" {
			http.Error(w, "Missing Destination header", http.StatusBadRequest)
			return
		}
		destURL, err := url.Parse(destHeader)
		if err != nil {
			http.Error(w, "Invalid Destination header: "+err.Error(), http.StatusBadRequest)
			return
		}
		destPath := destURL.Path
		log.Printf("Moving %s to %s", path, destPath)
		err = fs.Move(path, destPath)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		log.Printf("Moved %s to %s", path, destPath)
		w.WriteHeader(http.StatusCreated)

	case "COPY":
		destHeader := r.Header.Get("Destination")
		if destHeader == "" {
			http.Error(w, "Missing Destination header", http.StatusBadRequest)
			return
		}
		destURL, err := url.Parse(destHeader)
		if err != nil {
			http.Error(w, "Invalid Destination header: "+err.Error(), http.StatusBadRequest)
			return
		}
		destPath := destURL.Path
		log.Printf("Copying %s to %s", path, destPath)
		err = fs.Copy(path, destPath)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		log.Printf("Copied %s to %s", path, destPath)
		w.WriteHeader(http.StatusCreated)

	case "PROPFIND":
		fileEntry, fileErr := fs.GetFile(path)
		dirEntry, dirErr := fs.GetDir(path)

		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)

		var responses []string
		if fileErr == nil {
			size := getFileSize(fileEntry.Content)
			created := time.Unix(fileEntry.Created, 0).UTC().Format(time.RFC1123)
			modified := time.Unix(fileEntry.Modified, 0).UTC().Format(time.RFC1123)
			responses = append(responses, fmt.Sprintf(`
    <response>
        <href>%s</href>
        <propstat>
            <prop>
                <getcontentlength>%d</getcontentlength>
                <resourcetype/>
                <creationdate>%s</creationdate>
                <getlastmodified>%s</getlastmodified>
            </prop>
            <status>HTTP/1.1 200 OK</status>
        </propstat>
    </response>`, path, size, created, modified))
		}
		if dirErr == nil || path == "/" {
			created := time.Now().Unix()
			modified := created
			if dirErr == nil {
				created = dirEntry.Created
				modified = dirEntry.Modified
			}
			createdStr := time.Unix(created, 0).UTC().Format(time.RFC1123)
			modifiedStr := time.Unix(modified, 0).UTC().Format(time.RFC1123)
			responses = append(responses, fmt.Sprintf(`
    <response>
        <href>%s</href>
        <propstat>
            <prop>
                <resourcetype><collection/></resourcetype>
                <creationdate>%s</creationdate>
                <getlastmodified>%s</getlastmodified>
            </prop>
            <status>HTTP/1.1 200 OK</status>
        </propstat>
    </response>`, path, createdStr, modifiedStr))

			children := fs.ListDir(path)
			log.Printf("Listing children for %s: %d items", path, len(children))
			for _, child := range children {
				if child.IsDir {
					created := time.Unix(child.Created, 0).UTC().Format(time.RFC1123)
					modified := time.Unix(child.Modified, 0).UTC().Format(time.RFC1123)
					responses = append(responses, fmt.Sprintf(`
    <response>
        <href>%s</href>
        <propstat>
            <prop>
                <resourcetype><collection/></resourcetype>
                <creationdate>%s</creationdate>
                <getlastmodified>%s</getlastmodified>
            </prop>
            <status>HTTP/1.1 200 OK</status>
        </propstat>
    </response>`, child.Path, created, modified))
				} else {
					size := getFileSize(child.Content)
					created := time.Unix(child.Created, 0).UTC().Format(time.RFC1123)
					modified := time.Unix(child.Modified, 0).UTC().Format(time.RFC1123)
					responses = append(responses, fmt.Sprintf(`
    <response>
        <href>%s</href>
        <propstat>
            <prop>
                <getcontentlength>%d</getcontentlength>
                <resourcetype/>
                <creationdate>%s</creationdate>
                <getlastmodified>%s</getlastmodified>
            </prop>
            <status>HTTP/1.1 200 OK</status>
        </propstat>
    </response>`, child.Path, size, created, modified))
				}
			}
		}

		if len(responses) == 0 {
			log.Printf("No entries found for %s", path)
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>
<multistatus xmlns="DAV:">%s
</multistatus>`, strings.Join(responses, "\n"))

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// 文件系统持久化
func loadFileSystem() *FileSystem {
	fs := &FileSystem{}
	if _, err := os.Stat(DATA_FILE); os.IsNotExist(err) {
		now := time.Now().Unix()
		fs.Files = append(fs.Files, FileEntry{
			Path:     "/",
			Content:  "",
			IsDir:    true,
			Created:  now,
			Modified: now,
		})
		return fs
	}

	file, err := os.Open(DATA_FILE)
	if err != nil {
		log.Printf("加载 %s 失败: %v", DATA_FILE, err)
		return fs
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(fs); err != nil {
		log.Printf("解析 %s 失败: %v", DATA_FILE, err)
		now := time.Now().Unix()
		return &FileSystem{
			Files: []FileEntry{
				{
					Path:     "/",
					Content:  "",
					IsDir:    true,
					Created:  now,
					Modified: now,
				},
			},
		}
	}
	return fs
}

func saveFileSystem(fs *FileSystem) {
	file, err := os.Create(DATA_FILE)
	if err != nil {
		log.Printf("保存 %s 失败: %v", DATA_FILE, err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(fs); err != nil {
		log.Printf("写入 %s 失败: %v", DATA_FILE, err)
	}
}

func hashMd5(data string) []byte {
	h := md5.New()
	h.Write([]byte(data))
	return h.Sum(nil)
}

func main() {
	http.HandleFunc("/", handleWebDAV)
	log.Printf("MixFileWebDAV已启动: %d", SERVER_PORT)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", SERVER_PORT), nil); err != nil {
		log.Fatal(err)
	}
}
