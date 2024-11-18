package ports

import "LogDb/internal/domain"

type Merger interface {
	// MergeDataPages merges two data pages into one.
	MergeDataPages(dp1, dp2 *domain.ReadOnlyDataPage) (*domain.DataPage, error)
	// MergeDataFiles merges two data files into one.
	MergeDataFiles(df1, df2 *domain.DataFile) (*domain.DataFile, error)
}
