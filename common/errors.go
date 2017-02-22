package common

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

/*
ErrTabletNotLoaded is an error to return when the key or key range
requested is not loaded on this server.
*/
var ErrTabletNotLoaded = grpc.Errorf(
	codes.Unavailable, "Tablet not loaded")

/*
ErrColumnFamilyNotConfigured is an error indicating that while the tablet
specified is known, the specific column family is not configured.
*/
var ErrColumnFamilyNotConfigured = grpc.Errorf(
	codes.NotFound, "Column family not configured")
