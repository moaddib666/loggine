package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"io/fs"
	"os"
	"path"
)

var _ ports.DataFileRepository = (*DataFileRepository)(nil)

type DataFileRepository struct {
	basePath string
	codec    ports.Serializer
	ext      string
}

// open opens a data file in the repository
func (d *DataFileRepository) open(fullPath string) (*domain.DataFile, error) {
	log.Debugf("Opening data file: %s", fullPath)
	fd, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	header := domain.NewEmptyDataFileHeader()
	if _, err = d.codec.ReadFileHeader(header, fd); err != nil {
		return nil, err
	}
	return domain.NewDataFile(header, fd), nil
}

func (d *DataFileRepository) Open(fileName string) (*domain.DataFile, error) {
	fullPath := d.GetDataFileFullPath(fileName)
	return d.open(fullPath)
}

// Delete deletes a data file from the repository
func (d *DataFileRepository) Delete(fileName string) error {
	fullPath := d.GetDataFileFullPath(fileName)
	log.Debugf("Deleting data file: %s", fullPath)
	return os.Remove(fullPath)
}

// DeleteByHeader deletes a data file from the repository by header
func (d *DataFileRepository) DeleteByHeader(header *domain.DataFileHeader) error {
	return d.Delete(header.String())
}

// GetDataFileFullPath constructs the path to the data file
func (d *DataFileRepository) GetDataFileFullPath(name string) string {
	if d.ext != "" {
		name += "." + d.ext
	}
	return path.Join(d.basePath, name)
}

// GetDataFileFullTempPath constructs the path to the temporary data file
func (d *DataFileRepository) GetDataFileFullTempPath(name string) string {
	return d.GetDataFileFullPath(name) + ".tmp"
}

// Create creates a new data file in the repository Create(y, m, day uint64)
func (d *DataFileRepository) Create(y, m, day uint64) (*domain.DataFile, error) {
	id := uuid.New().ID()
	dataFileHeader := domain.NewDataFileHeader(1, id, y, m, day)
	return d.CreateFromHeader(dataFileHeader)
}

// CreateFromHeader creates a new data file in the repository from a header
func (d *DataFileRepository) CreateFromHeader(header *domain.DataFileHeader) (*domain.DataFile, error) {
	log.Debugf("Creating data file: %s", header)
	return domain.NewWriteOnlyDataFile(header, d.GetDataFileFullPath(header.String()))
	// TODO: automatically add header to the file
}

// CreateTempFromHeader creates a new temporary data file in the repository from a header
func (d *DataFileRepository) CreateTempFromHeader(header *domain.DataFileHeader) (*domain.DataFile, error) {
	log.Debugf("Creating temporary data file: %s", header)
	df, err := domain.NewWriteOnlyDataFile(header, d.GetDataFileFullTempPath(header.String()))
	if err != nil {
		return nil, err
	}
	if _, err := df.File.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := d.codec.WriteFileHeader(header, df); err != nil {
		return nil, err
	}
	return df, nil
}

// MakePermanentFromHeader marks a temporary data file as final
func (d *DataFileRepository) MakePermanentFromHeader(tempFile *domain.DataFile) error {
	log.Debugf("Marking temporary data file as final: %s", tempFile.Header)
	return os.Rename(d.GetDataFileFullTempPath(tempFile.Header.String()), d.GetDataFileFullPath(tempFile.Header.String()))
}

// Codec returns the codec used by the repository
func (d *DataFileRepository) Codec() ports.Serializer {
	return d.codec
}

// FileExtension returns the file extension used by the repository
func (d *DataFileRepository) FileExtension() string {
	return d.ext
}

// BasePath returns the base path used by the repository
func (d *DataFileRepository) BasePath() string {
	return d.basePath
}

// ListAvailable returns the list of available files in the repository
func (d *DataFileRepository) ListAvailable() ([]*domain.DataFileHeader, error) {
	log.Debugf("Loading data files from directory: %s", d.basePath)
	files, err := fs.Glob(os.DirFS(d.basePath), "*."+d.ext)
	if err != nil {
		return nil, err
	}
	var dataFiles []*domain.DataFileHeader
	for _, file := range files {
		fullPath := path.Join(d.basePath, file)
		df, err := d.open(fullPath)
		if err != nil {
			log.WithError(err).Errorf("Failed to open data file %s", fullPath)
			continue
		}
		dataFiles = append(dataFiles, df.Header)
		_ = df.Close()
	}
	return dataFiles, nil
}

// NewDataFileRepository creates a new DataFileRepository
func NewDataFileRepository(basePath string, codec ports.Serializer, ext string) *DataFileRepository {
	if err := os.MkdirAll(basePath, 0700); err != nil {
		panic(err)
	}
	return &DataFileRepository{
		basePath: basePath,
		codec:    codec,
		ext:      ext,
	}
}
