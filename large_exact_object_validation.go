package s3ext

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"strings"
	"unicode"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vmihailenco/msgpack/v5"
)

func largeExactObjectValidationWire(ctx context.Context, state *artifactValidationState, wire []byte) ([]byte, uint32) {
	var command effect.StorageLargeExactObjectValidationCommandV1
	reader := bytes.NewReader(wire)
	decoder := msgpack.NewDecoder(reader)
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&command); err != nil || reader.Len() != 0 {
		return nil, artifactValidationCodeDecode
	}
	if err := command.Validate(); err != nil {
		return nil, artifactValidationCodeInvalid
	}
	result, err := state.validateLargeExactObject(ctx, command)
	if err != nil {
		return nil, artifactValidationErrorCode(err)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return nil, artifactValidationCodeEncode
	}
	return encoded, artifactValidationCodeOK
}

func (s *artifactValidationState) validateLargeExactObject(
	ctx context.Context,
	command effect.StorageLargeExactObjectValidationCommandV1,
) (effect.StorageLargeExactObjectValidationResultV1, error) {
	if s == nil || s.client == nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, errors.New("storage artifact validation: provider unavailable")
	}
	if err := command.Validate(); err != nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, fmt.Errorf("%w: %v", errArtifactInvalid, err)
	}
	if err := s.client.ensureClient(); err != nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, fmt.Errorf("storage artifact validation: client: %w", err)
	}
	if _, err := s.headLargeExactObject(ctx, command); err != nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, err
	}

	get := s.getObject
	if get == nil {
		get = func(ctx context.Context, state *s3ClientState, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			return state.client.GetObject(ctx, input)
		}
	}
	output, err := get(ctx, s.client, &s3.GetObjectInput{
		Bucket:  aws.String(s.client.bucket),
		Key:     aws.String(command.ObjectKey),
		IfMatch: quotedETag(command.ExpectedGeneration),
	})
	if err != nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, classifyArtifactObjectReadError(command.ObjectKey, err)
	}
	if output == nil || output.Body == nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, fmt.Errorf("%w: object read returned no body", errArtifactInvalid)
	}
	defer output.Body.Close()
	if output.ContentLength == nil || *output.ContentLength != command.ContentLength ||
		*output.ContentLength > command.Limits.MaxContentBytes {
		return effect.StorageLargeExactObjectValidationResultV1{}, fmt.Errorf("%w: observed content length does not match claim", errArtifactInvalid)
	}
	if generation := normalizeETag(output.ETag); generation == "" ||
		generation != normalizeETagValue(command.ExpectedGeneration) {
		return effect.StorageLargeExactObjectValidationResultV1{}, errArtifactGenerationMismatch
	}

	staging, err := os.CreateTemp("", "pulp-exact-object-*.zip")
	if err != nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, fmt.Errorf("storage artifact validation: create bounded staging file: %w", err)
	}
	stagingName := staging.Name()
	defer os.Remove(stagingName)
	defer staging.Close()

	hash := sha256.New()
	buffer := make([]byte, 128<<10)
	written, copyErr := io.CopyBuffer(
		io.MultiWriter(staging, hash),
		io.LimitReader(output.Body, command.Limits.MaxContentBytes+1),
		buffer,
	)
	if copyErr != nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, fmt.Errorf("storage artifact validation: stream object: %w", copyErr)
	}
	if written != command.ContentLength || written > command.Limits.MaxContentBytes {
		return effect.StorageLargeExactObjectValidationResultV1{}, fmt.Errorf("%w: streamed body violates claimed length", errArtifactInvalid)
	}
	observedSHA := hex.EncodeToString(hash.Sum(nil))

	// A second conditional HEAD closes the interval around the stream. This is
	// deliberately retained even though the GET was conditional: it prevents a
	// future provider adapter from weakening the generation fence unnoticed.
	if _, err := s.headLargeExactObject(ctx, command); err != nil {
		return effect.StorageLargeExactObjectValidationResultV1{}, err
	}

	result := effect.StorageLargeExactObjectValidationResultV1{
		ContractVersion:       command.ContractVersion,
		Claim:                 command.Claim,
		ObjectNamespace:       command.ObjectNamespace,
		ObjectKey:             command.ObjectKey,
		StorageGeneration:     command.ExpectedGeneration,
		ObservedContentLength: written,
		ExpectedSHA256:        command.ExpectedSHA256,
		ObservedSHA256:        observedSHA,
		ValidatorPolicy:       command.ValidatorPolicy,
	}
	if observedSHA != command.ExpectedSHA256 {
		result.ErrorCode = "sha256_mismatch"
		return result, nil
	}
	if err := validateLargeExactObjectZIP(staging, written, command.Limits); err != nil {
		result.ErrorCode = artifactValidationErrorName(err)
		return result, nil
	}
	result.Valid = true
	result.Object = &effect.StorageObjectGenerationRefV1{
		Version:    effect.StorageObjectGenerationRefVersionV1,
		Namespace:  command.ObjectNamespace,
		Key:        command.ObjectKey,
		Generation: 1,
		SHA256:     observedSHA,
		SizeBytes:  written,
	}
	return result, nil
}

func (s *artifactValidationState) headLargeExactObject(
	ctx context.Context,
	command effect.StorageLargeExactObjectValidationCommandV1,
) (*s3.HeadObjectOutput, error) {
	head := s.headObject
	if head == nil {
		head = func(ctx context.Context, state *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return state.client.HeadObject(ctx, input)
		}
	}
	output, err := head(ctx, s.client, &s3.HeadObjectInput{
		Bucket:  aws.String(s.client.bucket),
		Key:     aws.String(command.ObjectKey),
		IfMatch: quotedETag(command.ExpectedGeneration),
	})
	if err != nil {
		return nil, classifyArtifactObjectReadError(command.ObjectKey, err)
	}
	if output == nil || output.ContentLength == nil || *output.ContentLength != command.ContentLength {
		return nil, fmt.Errorf("%w: HEAD content length does not match claim", errArtifactInvalid)
	}
	if generation := normalizeETag(output.ETag); generation == "" ||
		generation != normalizeETagValue(command.ExpectedGeneration) {
		return nil, errArtifactGenerationMismatch
	}
	return output, nil
}

func classifyArtifactObjectReadError(key string, err error) error {
	if classifyS3Error(err) == scopedObjectStoreNotFoundCode {
		return fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	if isArtifactPreconditionFailure(err) {
		return errArtifactGenerationMismatch
	}
	return fmt.Errorf("storage artifact validation: read %q: %w", key, err)
}

func validateLargeExactObjectZIP(
	file *os.File,
	size int64,
	limits effect.StorageLargeExactObjectValidationLimitsV1,
) error {
	if file == nil || size <= 0 || size > limits.MaxContentBytes {
		return fmt.Errorf("zip_invalid")
	}
	reader, err := zip.NewReader(file, size)
	if err != nil {
		return fmt.Errorf("zip_invalid")
	}
	if len(reader.File) == 0 || len(reader.File) > int(limits.MaxEntries) {
		return fmt.Errorf("zip_entry_count")
	}
	seen := make(map[string]struct{}, len(reader.File))
	var totalUncompressed uint64
	var totalCompressed uint64
	var regularFiles uint32
	for _, entry := range reader.File {
		if err := validateLargeExactObjectZIPEntry(entry); err != nil {
			return err
		}
		name := strings.TrimSuffix(entry.Name, "/")
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("zip_duplicate_path")
		}
		seen[name] = struct{}{}
		if entry.UncompressedSize64 > uint64(limits.MaxEntryUncompressedBytes) {
			return fmt.Errorf("zip_entry_size")
		}
		if totalUncompressed > uint64(limits.MaxTotalUncompressedBytes)-entry.UncompressedSize64 {
			return fmt.Errorf("zip_total_size")
		}
		totalUncompressed += entry.UncompressedSize64
		if totalCompressed > math.MaxUint64-entry.CompressedSize64 {
			return fmt.Errorf("zip_invalid")
		}
		totalCompressed += entry.CompressedSize64
		if entry.UncompressedSize64 > 0 &&
			(entry.CompressedSize64 == 0 ||
				exceedsCompressionRatio(entry.UncompressedSize64, entry.CompressedSize64, limits.MaxCompressionRatio)) {
			return fmt.Errorf("zip_compression_ratio")
		}
		if !entry.FileInfo().IsDir() {
			regularFiles++
			stream, err := entry.Open()
			if err != nil {
				return fmt.Errorf("zip_entry_open")
			}
			var probe [1]byte
			_, readErr := stream.Read(probe[:])
			closeErr := stream.Close()
			if readErr != nil && readErr != io.EOF || closeErr != nil {
				return fmt.Errorf("zip_entry_read")
			}
		}
	}
	if regularFiles == 0 {
		return fmt.Errorf("zip_no_files")
	}
	if totalUncompressed > 0 &&
		(totalCompressed == 0 ||
			exceedsCompressionRatio(totalUncompressed, totalCompressed, limits.MaxCompressionRatio)) {
		return fmt.Errorf("zip_compression_ratio")
	}
	return nil
}

func exceedsCompressionRatio(uncompressed, compressed uint64, ratio uint32) bool {
	if compressed == 0 {
		return uncompressed > 0
	}
	if ratio == 0 {
		return true
	}
	if compressed > math.MaxUint64/uint64(ratio) {
		return false
	}
	return uncompressed > compressed*uint64(ratio)
}

func validateLargeExactObjectZIPEntry(entry *zip.File) error {
	if entry == nil || entry.Name == "" || entry.Flags&1 != 0 ||
		strings.Contains(entry.Name, "\\") || strings.Contains(entry.Name, ":") ||
		strings.HasPrefix(entry.Name, "/") || strings.IndexFunc(entry.Name, unicode.IsControl) >= 0 {
		return fmt.Errorf("zip_path")
	}
	name := strings.TrimSuffix(entry.Name, "/")
	if name == "" || name == "." || path.Clean(name) != name || strings.HasPrefix(name, "../") {
		return fmt.Errorf("zip_path")
	}
	mode := entry.FileInfo().Mode()
	if mode&fs.ModeSymlink != 0 {
		return fmt.Errorf("zip_symlink")
	}
	if mode&fs.ModeType != 0 && !mode.IsDir() {
		return fmt.Errorf("zip_special_file")
	}
	return nil
}
