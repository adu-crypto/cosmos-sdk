package server

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
	"github.com/spf13/cobra"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server/types"
	"github.com/cosmos/cosmos-sdk/store"
	"github.com/cosmos/cosmos-sdk/store/rootmulti"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
)

const (
	FlagDBType = "app-db-backend"
)

// GetPruningOptionsFromFlags parses command flags and returns the correct
// PruningOptions. If a pruning strategy is provided, that will be parsed and
// returned, otherwise, it is assumed custom pruning options are provided.
func GetPruningOptionsFromFlags(appOpts types.AppOptions) (storetypes.PruningOptions, error) {
	strategy := strings.ToLower(cast.ToString(appOpts.Get(FlagPruning)))

	switch strategy {
	case storetypes.PruningOptionDefault, storetypes.PruningOptionNothing, storetypes.PruningOptionEverything:
		return storetypes.NewPruningOptionsFromString(strategy), nil

	case storetypes.PruningOptionCustom:
		opts := storetypes.NewPruningOptions(
			cast.ToUint64(appOpts.Get(FlagPruningKeepRecent)),
			cast.ToUint64(appOpts.Get(FlagPruningKeepEvery)),
			cast.ToUint64(appOpts.Get(FlagPruningInterval)),
		)

		if err := opts.Validate(); err != nil {
			return opts, fmt.Errorf("invalid custom pruning options: %w", err)
		}

		return opts, nil

	default:
		return store.PruningOptions{}, fmt.Errorf("unknown pruning strategy %s", strategy)
	}
}

// PruningCmd prunes the sdk root multi store history versions based on the pruning options
// specified by command flags.
func PruningCmd(providerCreator types.StoreProviderCreator) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "prune history stetes based on the pruning options specified by flags",
		Long: `Pruning options can be provided via the '--pruning' flag or alternatively with '--pruning-keep-recent', and
		'pruning-interval' together.
		
		For '--pruning' the options are as follows:
		
		default: the last 362880 states are kept, pruning at 10 block intervals
		nothing: all historic states will be saved, nothing will be deleted (i.e. archiving node)
		everything: 2 latest states will be kept; pruning at 10 block intervals.
		custom: allow pruning options to be manually specified through 'pruning-keep-recent', and 'pruning-interval'
		`,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			serverCtx := GetServerContextFromCmd(cmd)

			// Bind flags to the Context's Viper so the app construction can set
			// options accordingly.
			if err := serverCtx.Viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}

			_, err := GetPruningOptionsFromFlags(serverCtx.Viper)
			return err
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := GetServerContextFromCmd(cmd)

			home := ctx.Viper.GetString(flags.FlagHome)

			db, err := openDB(home)
			if err != nil {
				return err
			}

			// we should set pruning options in providerCreator
			provider := providerCreator(ctx.Logger, db, ctx.Viper)
			cms := provider.CommitMultiStore()
			cmsOptions := cms.GetPruning()
			// set pruning options for cms in case we forgot to apply the pruning options in providerCreator
			if cmsOptions.Interval == 0 && cmsOptions.KeepRecent == 0 {
				pruningOptions, err := GetPruningOptionsFromFlags(ctx.Viper)
				if err != nil {
					return err
				}
				cms.SetPruning(pruningOptions)
			}

			if rootMultiStore, ok := cms.(*rootmulti.Store); ok {
				err = rootMultiStore.PruneHistoryVersions()
				return err
			}

			return fmt.Errorf("currently only support the pruning of rootmulti.Store type")
		},
	}

	cmd.Flags().String(flags.FlagHome, "", "The database home directory")
	cmd.Flags().String(FlagPruning, storetypes.PruningOptionDefault, "Pruning strategy (default|nothing|everything|custom)")
	cmd.Flags().Uint64(FlagPruningKeepRecent, 0, "Number of recent heights to keep on disk (ignored if pruning is not 'custom')")
	cmd.Flags().Uint64(FlagPruningInterval, 0, "Height interval at which pruned heights are removed from disk (ignored if pruning is not 'custom')")
	cmd.Flags().String(FlagDBType, "", "the backend db type")

	return cmd
}
