package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
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
	fullPath := d.constructDataFileLocation(fileName)
	return d.open(fullPath)
}

// constructDataFileLocation constructs the path to the data file
func (d *DataFileRepository) constructDataFileLocation(name string) string {
	if d.ext != "" {
		name += "." + d.ext
	}
	return path.Join(d.basePath, name)
}

// Create creates a new data file in the repository Create(y, m, day uint64)
func (d *DataFileRepository) Create(y, m, day uint64) (*domain.DataFile, error) {
	id := uuid.New().ID()
	dataFileHeader := domain.NewDataFileHeader(1, id, y, m, day)
	log.Debugf("Creating data file: %s", dataFileHeader)
	return domain.NewWriteOnlyDataFile(dataFileHeader, d.constructDataFileLocation(dataFileHeader.String()))
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
